package Fetch

import (
	"github.com/mkocikowski/libkafka/api"
)

type Args struct {
	ClientId      string
	Topic         string
	Partition     int32
	Offset        int64
	MinBytes      int32
	MaxBytes      int32
	MaxWaitTimeMs int32
}

func NewRequest(args *Args) *api.Request {
	p := Partition{
		Partition:         args.Partition,
		FetchOffset:       args.Offset,
		PartitionMaxBytes: args.MaxBytes,
	}
	t := Topic{
		Topic:      args.Topic,
		Partitions: []Partition{p, p},
	}
	return &api.Request{
		ApiKey:        api.Fetch,
		ApiVersion:    6,
		CorrelationId: 0,
		ClientId:      args.ClientId,
		Body: Request{
			ReplicaId:     -1,
			MaxWaitTimeMs: args.MaxWaitTimeMs,
			MinBytes:      args.MinBytes,
			MaxBytes:      args.MaxBytes,
			Topics:        []Topic{t},
		},
	}
}

type Request struct {
	ReplicaId      int32
	MaxWaitTimeMs  int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8 // not used
	Topics         []Topic
}

type Topic struct {
	Topic      string
	Partitions []Partition
}

type Partition struct {
	Partition         int32
	FetchOffset       int64
	LogStartOffset    int64 // not used
	PartitionMaxBytes int32
}
