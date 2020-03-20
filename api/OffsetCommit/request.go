package OffsetCommit

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(group, topic string, partition int32, offset, retentionMs int64) *api.Request {
	p := Partition{
		PartitionIndex:   partition,
		CommitedOffset:   offset,
		CommitedMetadata: "",
	}
	t := Topic{
		Name:       topic,
		Partitions: []Partition{p},
	}
	return &api.Request{
		ApiKey:     api.OffsetCommit,
		ApiVersion: 2,
		Body: Request{
			GroupId:         group,
			GenerationId:    -1,
			MemberId:        "",
			RetentionTimeMs: retentionMs,
			Topics:          []Topic{t},
		},
	}
}

type Request struct {
	GroupId         string
	GenerationId    int32
	MemberId        string
	RetentionTimeMs int64
	Topics          []Topic
}

type Topic struct {
	Name       string
	Partitions []Partition
}

type Partition struct {
	PartitionIndex   int32
	CommitedOffset   int64
	CommitedMetadata string
}
