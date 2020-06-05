package ListOffsets

import (
	"github.com/mkocikowski/libkafka/api"
)

// timestamp is milliseconds since epoch
func NewRequest(topic string, partition int32, timestampMs int64) *api.Request {
	p := []RequestPartition{{Partition: partition, Timestamp: timestampMs}}
	t := []RequestTopic{{Topic: topic, Partitions: p}}
	return &api.Request{
		ApiKey:     api.ListOffsets,
		ApiVersion: 2,
		Body: RequestBody{
			ReplicaId:      -1,
			IsolationLevel: 0,
			Topics:         t,
		},
	}
}

type RequestBody struct {
	ReplicaId      int32
	IsolationLevel int8
	Topics         []RequestTopic
}

type RequestTopic struct {
	Topic      string
	Partitions []RequestPartition
}

type RequestPartition struct {
	Partition int32
	Timestamp int64
}
