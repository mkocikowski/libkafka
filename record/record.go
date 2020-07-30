// Package record implements functions for marshaling and unmarshaling individual Kafka records.
package record

import (
	"encoding/binary"
	"io"

	"github.com/mkocikowski/libkafka/varint"
)

func Unmarshal(b []byte) (*Record, error) { // TODO: errors
	r := &Record{}
	var offset, n int
	r.Len, offset = varint.DecodeZigZag64(b)
	r.Attributes = int8(b[offset])
	offset += 1
	r.TimestampDelta, n = varint.DecodeZigZag64(b[offset:])
	offset += n
	r.OffsetDelta, n = varint.DecodeZigZag64(b[offset:])
	offset += n
	r.KeyLen, n = varint.DecodeZigZag64(b[offset:])
	offset += n
	// TODO: remove copy
	if r.KeyLen > 0 {
		r.Key = make([]byte, r.KeyLen)
	}
	offset += copy(r.Key, b[offset:])
	r.ValueLen, n = varint.DecodeZigZag64(b[offset:])
	if r.ValueLen < 1 {
		return r, nil
	}
	offset += n
	r.Value = make([]byte, r.ValueLen)
	n += copy(r.Value, b[offset:])
	return r, nil // TODO: errors
}

func New(key, value []byte) *Record {
	return &Record{
		KeyLen:   int64(len(key)),
		Key:      key,
		ValueLen: int64(len(value)),
		Value:    value,
	}
}

type Record struct {
	Len            int64
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	KeyLen         int64
	Key            []byte
	ValueLen       int64
	Value          []byte
	// TODO: headers
}

func (r *Record) Marshal() []byte {
	var b, c []byte
	buf := make([]byte, binary.MaxVarintLen64)
	b = varint.PutZigZag64(b, buf, int64(r.Attributes))
	b = varint.PutZigZag64(b, buf, r.TimestampDelta)
	b = varint.PutZigZag64(b, buf, r.OffsetDelta)
	b = varint.PutZigZag64(b, buf, r.KeyLen)
	b = append(b, r.Key...)
	b = varint.PutZigZag64(b, buf, r.ValueLen)
	b = append(b, r.Value...)
	b = varint.PutZigZag64(b, buf, 0) // no headers
	c = varint.PutZigZag64(c, buf, int64(len(b)))
	c = append(c, b...)
	return c
}

func (r *Record) Marshal2(b []byte) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	b = varint.PutZigZag64(b, buf, int64(r.Attributes))
	b = varint.PutZigZag64(b, buf, r.TimestampDelta)
	b = varint.PutZigZag64(b, buf, r.OffsetDelta)
	b = varint.PutZigZag64(b, buf, r.KeyLen)
	b = append(b, r.Key...)
	b = varint.PutZigZag64(b, buf, r.ValueLen)
	b = append(b, r.Value...)
	b = varint.PutZigZag64(b, buf, 0) // no headers
	c := make([]byte, 0, len(b)+10)
	c = varint.PutZigZag64(c, buf, int64(len(b)))
	c = append(c, b...)
	return c
}

func (r *Record) Marshal3() []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	// allocate only once, but enough to fit "most cases"
	b := make([]byte, len(r.Key)+len(r.Value)+100)
	// "reserve" 10 bytes to leave room for the record length
	b = b[0:binary.MaxVarintLen64]
	// write out the record
	b = varint.PutZigZag64(b, buf, int64(r.Attributes))
	b = varint.PutZigZag64(b, buf, r.TimestampDelta)
	b = varint.PutZigZag64(b, buf, r.OffsetDelta)
	b = varint.PutZigZag64(b, buf, r.KeyLen)
	b = append(b, r.Key...)
	b = varint.PutZigZag64(b, buf, r.ValueLen)
	b = append(b, r.Value...)
	b = varint.PutZigZag64(b, buf, 0) // no headers
	// write out record length
	c := make([]byte, 0, binary.MaxVarintLen64)
	c = varint.PutZigZag64(c, buf, int64(len(b)-binary.MaxVarintLen64))
	// and put while record lenght "in front" using "reserved" space
	offset := binary.MaxVarintLen64 - len(c)
	copy(b[offset:], c)
	return b[offset:len(b)]
}

func (r *Record) Marshal4(tmp, header []byte, dst io.Writer) {
	header = header[:0] // reset because it will be appended to
	header = varint.PutZigZag64(header, tmp, int64(r.Attributes))
	header = varint.PutZigZag64(header, tmp, r.TimestampDelta)
	header = varint.PutZigZag64(header, tmp, r.OffsetDelta)
	header = varint.PutZigZag64(header, tmp, r.KeyLen)
	header = append(header, r.Key...)
	header = varint.PutZigZag64(header, tmp, r.ValueLen)
	//
	length := int64(len(header) + len(r.Value) + 1)
	n := varint.PutVarint(tmp, uint64(length<<1^(length>>63))) // ZigZag
	dst.Write(tmp[:n])
	dst.Write(header)
	dst.Write(r.Value)
	dst.Write([]byte{0}) // no kafka record "headers"
}
