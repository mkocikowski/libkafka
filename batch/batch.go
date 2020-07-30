/*
Package batch implements functions for building, marshaling, and unmarshaling
Kafka record batches.

Producing

When producting messages, call NewBuilder, and Add records to it. Call
Builder.Build and pass the returned Batch to the producer. Release the
reference to Builder when done with it to release references to added records.

Fetching ("consuming")

Fetch result (if successful) will contain RecordSet. Call its Batches method to
get byte slices containing individual batches. Unmarshal each batch
individually. To get individual records, call Batch.Records and then
record.Unmarshal. Passing around batches is much more efficient than passing
individual records, so save record unmarshaling until the very end.
*/
package batch

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"time"

	"github.com/mkocikowski/libkafka/compression"
	"github.com/mkocikowski/libkafka/record"
	"github.com/mkocikowski/libkafka/varint"
	"github.com/mkocikowski/libkafka/wire"
)

type Compressor interface {
	Compress([]byte) ([]byte, error)
	Type() int16
}

type Decompressor interface {
	Decompress([]byte) ([]byte, error)
	Type() int16
}

func NewBuilder(now time.Time) *Builder {
	return &Builder{t: now}
}

// Builder is used for building record batches. There is no limit on the number
// of records (up to the user). Not safe for concurrent use.
type Builder struct {
	t       time.Time
	records []*record.Record
}

// Add records to the batch. References to added records are not released on
// call to Build. This means you can add more records and call Build again.
// Don't know why you would want to, but you can.
func (b *Builder) Add(records ...*record.Record) {
	b.records = append(b.records, records...)
}

func (b *Builder) AddStrings(values ...string) *Builder {
	for _, s := range values {
		b.records = append(b.records, record.New(nil, []byte(s)))
	}
	return b
}

// NumRecords that have been added to the builder.
func (b *Builder) NumRecords() int {
	return len(b.records)
}

var (
	ErrEmpty     = errors.New("empty batch")
	ErrNilRecord = errors.New("nil record in batch")
)

// Build a record batch (marshal individual records and set batch metadata).
// Call this after adding records to the batch. Returns ErrEmpty if batch has
// no records. Returns ErrNilRecord if any of the records is nil. Marshaled
// records are not compressed (call Batch.Compress). Batch FirstTimestamp is
// set to the time when the builder was created (with NewBuilder) and the
// MaxTimestamp is set to the time passed to Build. Within the batch, each
// record's TimestampDelta is 0, meaning that all records will appear to have
// been produced at the time the builder was created (TODO: change? how?)
// Idempotent.
func (b *Builder) Build(now time.Time) (*Batch, error) {
	if len(b.records) == 0 {
		return nil, ErrEmpty
	}
	tmp := make([]byte, binary.MaxVarintLen64)
	header := make([]byte, 1<<10)
	buf := new(bytes.Buffer)
	for i, r := range b.records {
		if r == nil {
			return nil, ErrNilRecord
		}
		r.OffsetDelta = int64(i)
		r.Marshal4(tmp, header, buf)
	}
	marshaledRecords := buf.Bytes()
	return &Batch{
		BatchLengthBytes: int32(49 + len(marshaledRecords)), // TODO: constant
		Magic:            2,
		Attributes:       compression.None,
		LastOffsetDelta:  int32(len(b.records) - 1),
		// TODO: base timestamps on record timestamps
		FirstTimestamp:   b.t.UnixNano() / int64(time.Millisecond),
		MaxTimestamp:     now.UnixNano() / int64(time.Millisecond),
		ProducerId:       -1,
		ProducerEpoch:    -1,
		NumRecords:       int32(len(b.records)),
		MarshaledRecords: marshaledRecords,
	}, nil
}

var (
	CorruptedBatchError = errors.New("batch crc does not match bytes")
	crc32c              = crc32.MakeTable(crc32.Castagnoli)
)

// Unmarshal the batch. On error batch is nil. If there is an error, it is most
// likely because the crc failed. In that case there is no way to tell how many
// records there were in the batch (and to adjust offsets accordingly).
func Unmarshal(b []byte) (*Batch, error) {
	buf := bytes.NewBuffer(b)
	batch := &Batch{}
	if err := wire.Read(buf, reflect.ValueOf(batch)); err != nil {
		return nil, err
	}
	batch.MarshaledRecords = buf.Bytes() // the remainder is the message bodies
	crc := crc32.Checksum(b[21:], crc32c)
	if crc != batch.Crc {
		return nil, CorruptedBatchError
	}
	return batch, nil
}

// Batch defines Kafka record batch in wire format. Not safe for concurrent use.
type Batch struct {
	BaseOffset           int64
	BatchLengthBytes     int32
	PartitionLeaderEpoch int32
	Magic                int8 // this should be =2
	Crc                  uint32
	Attributes           int16
	LastOffsetDelta      int32 // NumRecords-1 // TODO: is this always true?
	FirstTimestamp       int64 // ms since epoch
	MaxTimestamp         int64 // ms since epoch
	ProducerId           int64 // for transactions only see KIP-360
	ProducerEpoch        int16 // for transactions only see KIP-360
	BaseSequence         int32
	NumRecords           int32 // LastOffsetDelta+1
	//
	MarshaledRecords []byte `wire:"omit" json:"-"`
}

func (batch *Batch) CompressionType() int16 {
	return batch.Attributes & 0b111
}

const (
	TimestampCreate    = 0b0000
	TimestampLogAppend = 0b1000
)

func (batch *Batch) TimestampType() int16 {
	return batch.Attributes & 0b1000
}

func (batch *Batch) LastOffset() int64 {
	return batch.BaseOffset + int64(batch.LastOffsetDelta)
}

// Marshal batch header and append marshaled records. If you want the batch to
// be compressed call Compress before Marshal. Mutates the batch Crc.
func (batch *Batch) Marshal() RecordSet {
	buf := new(bytes.Buffer)
	if err := wire.Write(buf, reflect.ValueOf(batch)); err != nil {
		panic(err)
	}
	buf.Write(batch.MarshaledRecords)
	b := buf.Bytes()
	batch.Crc = crc32.Checksum(b[21:], crc32c)
	buf = new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, batch.Crc)
	copy(b[17:], buf.Bytes())
	return b
}

// Compress batch records with supplied compressor. Mutates batch on success
// only. Call before Marshal. Not idempotent (on success).
func (batch *Batch) Compress(c Compressor) error {
	b, err := c.Compress(batch.MarshaledRecords)
	if err != nil {
		return fmt.Errorf("error compressing batch records: %w", err)
	}
	batch.BatchLengthBytes = int32(49 + len(b)) // TODO: constant
	batch.Attributes = c.Type()
	batch.Crc = 0 // invalidate crc
	batch.MarshaledRecords = b
	return nil
}

// Decompress batch with supplied decompressor. Mutates batch. Call after
// Unmarshal and before Records. Not idempotent.
func (batch *Batch) Decompress(d Decompressor) error {
	b, err := d.Decompress(batch.MarshaledRecords)
	if err != nil {
		return fmt.Errorf("error decompressing record batch: %w", err)
	}
	batch.BatchLengthBytes = int32(49 + len(b)) // TODO: constant
	batch.Attributes = compression.None
	batch.Crc = 0 // invalidate crc
	batch.MarshaledRecords = b
	return nil
}

// Records retrieves individual records from the batch. If batch records are
// compressed you must call Decompress first.
func (batch *Batch) Records() [][]byte {
	var records [][]byte
	for b := batch.MarshaledRecords; len(b) > 0; {
		length, n := varint.DecodeZigZag64(b)
		n += int(length)
		records = append(records, b[0:n])
		b = b[n:]
	}
	return records
}

// RecordSet is composed of 1 or more record batches. Fetch API calls respond
// with record sets. Byte representation of a record set with only one record
// batch is identical to the record batch.
type RecordSet []byte

// Batches returns the batches in the record set. Because Kafka limits response
// byte sizes, the last record batch in the set may be truncated (bytes will be
// missing from the end). In such case the last batch is discarded.
func (b RecordSet) Batches() [][]byte {
	var batches [][]byte
	var offset int64
	var length int32
	for {
		if len(b) == 0 {
			break
		}
		r := bytes.NewReader(b)
		if err := binary.Read(r, binary.BigEndian, &offset); err != nil {
			break
		}
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			break
		}
		n := int(length + 8 + 4)
		if len(b) < n {
			break // "incomplete" batch
		}
		batches = append(batches, b[:n])
		b = b[n:]
	}
	return batches
}
