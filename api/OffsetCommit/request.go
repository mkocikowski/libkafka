package OffsetCommit

import (
	"github.com/mkocikowski/libkafka/api"
)

// NewRequest constructs api.Request with ApiKey OffsetCommit. Method supports
// multiply offsets to be commited at once by providing them in offsets variable
// which is a map partition -> offset.
func NewRequest(group, topic string, offsets map[int32]int64, retentionMs int64) *api.Request {
	offsetsMap := make([]Partition, len(offsets))
	i := 0
	for p, o := range offsets {
		offsetsMap[i] = Partition{
			PartitionIndex:   p,
			CommitedOffset:   o,
			CommitedMetadata: "",
		}
		i++
	}
	t := Topic{
		Name:       topic,
		Partitions: offsetsMap,
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
