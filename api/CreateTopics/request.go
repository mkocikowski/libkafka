package CreateTopics

import (
	"github.com/mkocikowski/libkafka/api"
)

/*
message.format.version 1.0
compression.type uncompressed
message.timestamp.type CreateTime|LogAppendTime
message.timestamp.difference.max.ms 1000 ignored if LogAppendTime
*/

func NewRequest(topic string, numPartitions int32, replicationFactor int16, configs []Config) *api.Request {
	t := Topic{
		Name:              topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Assignments:       []Assignment{},
		Configs:           configs,
	}
	return &api.Request{
		ApiKey:     api.CreateTopics,
		ApiVersion: 2,
		Body: Request{
			Topics:       []Topic{t},
			TimeoutMs:    1000,
			ValidateOnly: false,
		},
	}
}

type Request struct {
	Topics       []Topic
	TimeoutMs    int32
	ValidateOnly bool
}

type Topic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Assignments       []Assignment
	Configs           []Config
}

type Assignment struct {
	PartitionIndex int32
	BrokerIds      []int32
}

type Config struct {
	Name  string
	Value string
}
