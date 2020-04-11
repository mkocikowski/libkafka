package Produce

import (
	"github.com/mkocikowski/libkafka/api"
)

type Args struct {
	ClientId  string
	Topic     string
	Partition int32
	Acks      int16 // 0: no, 1: leader only, -1: all ISRs (as specified by min.insync.replicas)
	TimeoutMs int32
}

func NewRequest(args *Args, recordSet []byte) *api.Request {
	d := Data{
		Partition: args.Partition,
		RecordSet: recordSet,
	}
	t := TopicData{
		Topic: args.Topic,
		Data:  []Data{d},
	}
	return &api.Request{
		ApiKey:        api.Produce,
		ApiVersion:    7,
		CorrelationId: 0,
		ClientId:      args.ClientId,
		Body: Request{
			TransactionalId: "",
			Acks:            args.Acks,
			TimeoutMs:       args.TimeoutMs,
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
