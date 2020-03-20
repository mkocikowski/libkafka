package OffsetFetch

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(group, topic string, partition int32) *api.Request {
	t := Topic{
		Name:             topic,
		PartitionIndexes: []int32{partition},
	}
	return &api.Request{
		ApiKey:     api.OffsetFetch,
		ApiVersion: 3,
		Body: Request{
			GroupId: group,
			Topics:  []Topic{t},
		},
	}
}

type Request struct {
	GroupId string
	Topics  []Topic
}

type Topic struct {
	Name             string
	PartitionIndexes []int32
}
