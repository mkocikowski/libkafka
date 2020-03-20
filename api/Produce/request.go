package Produce

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(topic string, partition int32, batch []byte) *api.Request {
	d := Data{
		Partition: partition,
		RecordSet: batch,
	}
	t := TopicData{
		Topic: topic,
		Data:  []Data{d},
	}
	return &api.Request{
		ApiKey:        api.Produce,
		ApiVersion:    7,
		CorrelationId: 0,
		ClientId:      "",
		Body: Request{
			TransactionalId: "",
			Acks:            1,
			TimeoutMs:       1000,
			TopicData:       []TopicData{t},
		},
	}
}

type Request struct {
	TransactionalId string // NULLABLE_STRING
	Acks            int16  // 0: no, 1: leader only, -1: all ISRs (as specified by min.insync.replicas)
	TimeoutMs       int32
	TopicData       []TopicData
}

type TopicData struct {
	Topic string
	Data  []Data
}

type Data struct {
	Partition int32
	RecordSet []byte
}
