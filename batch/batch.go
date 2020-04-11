/*
Package batch implements functions for building, marshaling, and unmarshaling
Kafka record batches.

Producing

When producting messages, call NewBuilder, and Add records to it. Call
Builder.Build and pass the returned Batch to the producer. Set the Builder to
nil when done with it to release references to added records.

Consuming

Fetch result (if successful) will contain RecordSet. Call its Batches method to
get byte slices containing individual batches.  Unmarshal each batch
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

	"github.com/mkocikowski/libkafka/record"
	"github.com/mkocikowski/libkafka/varint"
	"github.com/mkocikowski/libkafka/wire"
)

func NewBuilder(now time.Time) *Builder {
	return &Builder{t: now}
}

// Builder is used for building record batches. There is no limit on the number
// of records (up to the user). Not safe for concurrent use.
type Builder struct {
	t       time.Time
	records []*record.Record
}

// Add record to the batch. References to added records are not released on
// call to Build.
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

// Compressor implementations are supplied to the Builder by the user. This
// gives the user flexibility (say in picking zstd Data Dog or Klaus Post
// implementations).
type Compressor interface {
	Compress([]byte) ([]byte, error)
	Type() int16
}

var ErrEmpty = errors.New("empty batch")

// Build a record batch. Call this after adding records to the batch.
// Serializes and compresses records. Does not set the Crc (this is done by
// Batch.Marshal). Returns ErrEmpty if batch has no records. Build is
// idempotent.
func (b *Builder) Build(now time.Time, c Compressor) (*Batch, error) {
	if len(b.records) == 0 {
		return nil, ErrEmpty
	}
	buf := new(bytes.Buffer)
	for i, r := range b.records {
		r.OffsetDelta = int64(i)
		buf.Write(r.Marshal())
	}
	marshaledRecords, err := c.Compress(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("error compressing batch records: %v", err)
	}
	return &Batch{
		BatchLengthBytes: int32(49 + len(marshaledRecords)), // TODO: constant
		Magic:            2,
		Attributes:       c.Type(),
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
	LastOffsetDelta      int32
	FirstTimestamp       int64 // ms since epoch
	MaxTimestamp         int64 // ms since epoch
	ProducerId           int64 // for transactions only see KIP-360
	ProducerEpoch        int16 // for transactions only see KIP-360
	BaseSequence         int32
	NumRecords           int32
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

// Marshal batch. Mutates the Crc.
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

// Ditto Compressor.
type Decompressor interface {
	Decompress([]byte) ([]byte, error)
	Type() int16
}

// Records returns byte slices containing decompressed records. It is up to the
// user to provide appropriate Decompressor based on the value of
// Batch.CompressionType.
func (batch *Batch) Records(d Decompressor) ([][]byte, error) {
	var records [][]byte
	b, err := d.Decompress(batch.MarshaledRecords)
	if err != nil {
		return nil, fmt.Errorf("error decompressing record batch: %v", err)
	}
	for {
		if len(b) == 0 {
			break
		}
		length, n := varint.DecodeZigZag64(b)
		n += int(length)
		records = append(records, b[0:n])
		b = b[n:]
	}
	return records, nil
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
			return nil
		}
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil
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
