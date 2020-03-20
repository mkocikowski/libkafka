// Package record implements functions for marshaling and unmarshaling individual Kafka records.
package record

import "github.com/mkocikowski/libkafka/varint"

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
	b = append(b, varint.EncodeZigZag64(int64(r.Attributes))...)
	b = append(b, varint.EncodeZigZag64(r.TimestampDelta)...)
	b = append(b, varint.EncodeZigZag64(r.OffsetDelta)...)
	b = append(b, varint.EncodeZigZag64(r.KeyLen)...)
	b = append(b, r.Key...)
	b = append(b, varint.EncodeZigZag64(r.ValueLen)...)
	b = append(b, r.Value...)
	b = append(b, varint.EncodeZigZag64(0)...) // no headers
	c = append(c, varint.EncodeZigZag64(int64(len(b)))...)
	c = append(c, b...)
	return c
}
