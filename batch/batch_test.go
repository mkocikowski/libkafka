package batch

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/compression"
	"github.com/mkocikowski/libkafka/record"
)

// this came from the wire from a live kafka 1.0 broker
const recordBatchFixture = `AAAAAAAAAAMAAABMAAAAAAJx8ZMnAAAAAAACAAABbZh/W
LMAAAFtmH9Ys/////////////8AAAAAAAAAAxAAAAABBG0xABAAAAIBBG0yABAAAAQBBG0zAA==`

func TestUnitUnmarshalRecordSet(t *testing.T) {
	fixture, _ := base64.StdEncoding.DecodeString(recordBatchFixture)
	batches := RecordSet(fixture).Batches()
	if n := len(batches); n != 1 {
		t.Fatal(n)
	}
	batch, err := Unmarshal(batches[0])
	if err != nil {
		t.Fatal(err)
	}
	if batch.Crc != 1911657255 {
		t.Fatal(batch.Crc)
	}
}

func TestUnitUnmarshalBatchFixture(t *testing.T) {
	fixture, _ := base64.StdEncoding.DecodeString(recordBatchFixture)
	batch, err := Unmarshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Crc != 1911657255 {
		t.Fatal(batch.Crc)
	}
	records, _ := batch.Records(&compression.Nop{})
	if len(records) != 3 {
		t.Fatal(len(records))
	}
	fixture[86] = 0xff // corrupt the fixture
	if _, err = Unmarshal(fixture); err != CorruptedBatchError {
		t.Fatal(err)
	}
}

func TestUnitMarshalBatch(t *testing.T) {
	now := time.Now()
	batch, _ := NewBuilder(now).AddStrings("m1", "m2", "m3").Build(now, &compression.Nop{})
	b := batch.Marshal()
	batch, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	records, err := batch.Records(&compression.Nop{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := record.Unmarshal(records[2])
	if string(r.Value) != "m3" {
		t.Fatal(string(r.Value))
	}
}

func TestUnitNumRecords(t *testing.T) {
	now := time.Now()
	builder := NewBuilder(now)
	if builder.NumRecords() != 0 {
		t.Fatal(builder.NumRecords())
	}
	builder.AddStrings("foo")
	if builder.NumRecords() != 1 {
		t.Fatal(builder.NumRecords())
	}
	batch, _ := builder.Build(now, &nopZstd{})
	if batch.NumRecords != 1 {
		t.Fatal(batch.NumRecords)
	}
}

type nopZstd struct{}

func (*nopZstd) Compress(b []byte) ([]byte, error)   { return b, nil }
func (*nopZstd) Decompress(b []byte) ([]byte, error) { return b, nil }
func (*nopZstd) Type() int16                         { return compression.Zstd }

func TestUnitBuild(t *testing.T) {
	now := time.Now()
	batch, _ := NewBuilder(now).AddStrings("m1", "m2", "m3").Build(now, &nopZstd{})
	if typ := batch.CompressionType(); typ != compression.Zstd {
		t.Fatal(typ)
	}
	records, err := batch.Records(&nopZstd{})
	if err != nil {
		t.Fatal(err)
	}
	r, _ := record.Unmarshal(records[2])
	if string(r.Value) != "m3" {
		t.Fatal(string(r.Value))
	}
	t.Logf("%+v", r)
}

func TestUnitBuildEmptyBatch(t *testing.T) {
	now := time.Now()
	batch, err := NewBuilder(now).Build(now, &nopZstd{})
	if err != ErrEmpty {
		t.Fatal(batch, err)
	}
}

const recordBodiesFixture = `EAAAAAEEbTEAEAAAAgEEbTIAEAAABAEEbTMA`

func TestUnitRecords(t *testing.T) {
	fixture, _ := base64.StdEncoding.DecodeString(recordBodiesFixture)
	batch := &Batch{MarshaledRecords: fixture}
	br, _ := batch.Records(&compression.Nop{})
	if len(br) != 3 {
		t.Fatal(len(br))
	}
	r, _ := record.Unmarshal(br[2])
	if string(r.Value) != "m3" {
		t.Fatal(string(r.Value))
	}
	t.Logf("%+v", br)
	for _, b := range br {
		r, _ := record.Unmarshal(b)
		t.Logf("%+v %s", r, base64.StdEncoding.EncodeToString(b))
	}
}

func TestUnitCompressionType(t *testing.T) {
	b := &Batch{Attributes: 12}
	if c := b.CompressionType(); c != compression.Zstd {
		t.Fatal(c)
	}
}

func TestUnitTimestampType(t *testing.T) {
	b := &Batch{Attributes: 12}
	if c := b.TimestampType(); c != TimestampLogAppend {
		t.Fatal(c)
	}
}
