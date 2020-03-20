package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"

	"github.com/mkocikowski/libkafka/wire"
)

func Read(r io.Reader) (*Response, error) {
	var size int32
	err := binary.Read(r, binary.BigEndian, &size)
	if err != nil {
		return nil, fmt.Errorf("error reading response size: %v", err)
	}
	b := make([]byte, int(size))
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}
	//log.Println(size, len(b))
	return &Response{body: b}, nil
}

type Response struct {
	body []byte
}

func (r *Response) CorrelationId() int32 {
	var c int32
	err := binary.Read(bytes.NewReader(r.body), binary.BigEndian, &c)
	if err != nil {
		panic(err)
	}
	return c
}

func (r *Response) Unmarshal(v interface{}) error {
	// [4:] skips bytes used for correlation id
	return wire.Read(bytes.NewReader(r.body[4:]), reflect.ValueOf(v))
}

func (r *Response) Bytes() []byte {
	// [4:] skips bytes used for correlation id
	return r.body[4:]
}
