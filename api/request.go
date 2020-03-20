package api

import (
	"bytes"
	"encoding/binary"
	"reflect"

	"github.com/mkocikowski/libkafka/wire"
)

// https://kafka.apache.org/protocol
// https://kafka.apache.org/documentation/#messageformat
// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

type Request struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
	Body          interface{}
}

func (r *Request) Bytes() []byte {
	tmp := new(bytes.Buffer)
	wire.Write(tmp, reflect.ValueOf(r))
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, int32(tmp.Len()))
	tmp.WriteTo(buf)
	return buf.Bytes()
}
