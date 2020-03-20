package Produce

import (
	"bytes"
	"reflect"

	"github.com/mkocikowski/libkafka/wire"
)

func UnmarshalResponse(b []byte) (*Response, error) {
	r := &Response{}
	buf := bytes.NewBuffer(b)
	err := wire.Read(buf, reflect.ValueOf(r))
	return r, err
}

type Response struct {
	TopicResponses []TopicResponse
	ThrottleTimeMs int32
}

type TopicResponse struct {
	Topic              string
	PartitionResponses []PartitionResponse
}

type PartitionResponse struct {
	Partition      int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}
