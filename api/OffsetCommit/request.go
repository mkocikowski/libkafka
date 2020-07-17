package OffsetCommit

import (
	"github.com/mkocikowski/libkafka/api"
)

// NewMultiplePartitionsRequest constructs api.Request with ApiKey
// "OffsetCommit" which is designed to flush offsets for multiple partitions at
// once. Variable retentionMs sets the time period in ms to retain the offsets
// (-1 means no limit)
func NewMultiplePartitionsRequest(group, topic string, offsets map[int32]int64, retentionMs int64) *api.Request {
	partitionOffsets := make([]Partition, len(offsets))
	i := 0
	for p, o := range offsets {
		partitionOffsets[i] = Partition{
			PartitionIndex: p,
			CommitedOffset: o,
		}
		i++
	}
	t := Topic{
		Name:       topic,
		Partitions: partitionOffsets,
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
