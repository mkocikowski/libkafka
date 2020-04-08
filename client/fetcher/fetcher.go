// Package fetcher implements a single partition Kafka fetcher. A "fetcher", in
// my nomenclature, is different from a "consumer" in that it does no offset
// management of its own: it doesn't even advance the offset on successfully
// reading a fetch response. The reason for this is that there are many nuanced
// error scenarios (example: fetch response successful; 3rd out of 5 returned
// batches is corrupted) and so it makes sense to push the error handling logic
// (and the logic responsible for advancing and storing offsets) to a higher
// level library or even to the user.
package fetcher

import (
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/errors"
)

func parseResponse(r *Fetch.Response) (*Response, error) {
	if n := len(r.TopicResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of topic responses: %d", n)
	}
	topicResponse := &(r.TopicResponses[0])
	if n := len(topicResponse.PartitionResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of partition responses: %d", n)
	}
	partitionResponse := &(topicResponse.PartitionResponses[0])
	return &Response{
		ThrottleTimeMs: r.ThrottleTimeMs,
		Topic:          topicResponse.Topic,
		Partition:      partitionResponse.Partition,
		ErrorCode:      partitionResponse.ErrorCode,
		LogStartOffset: partitionResponse.LogStartOffset,
		HighWatermark:  partitionResponse.HighWatermark,
		RecordSet:      batch.RecordSet(partitionResponse.RecordSet),
	}, nil
}

type Response struct {
	Topic          string
	Partition      int32
	ThrottleTimeMs int32
	ErrorCode      int16
	LogStartOffset int64
	HighWatermark  int64
	RecordSet      batch.RecordSet
}

type PartitionFetcher struct {
	sync.Mutex
	client.PartitionClient
	Offset int64
}

var (
	MessageNewest = time.Unix(0, -1e6)
	MessageOldest = time.Unix(0, -2e6)
)

func (c *PartitionFetcher) Seek(offset time.Time) error {
	o := offset.UnixNano() / int64(time.Millisecond)
	resp, err := c.PartitionClient.ListOffsets(o)
	if err != nil {
		return err
	}
	p := resp.Responses[0].Partitions[0]
	if p.ErrorCode != errors.NONE {
		return &errors.KafkaError{Code: p.ErrorCode}
	}
	c.Lock()
	c.Offset = p.Offset
	c.Unlock()
	return nil
}

func (c *PartitionFetcher) SetOffset(offset int64) {
	c.Lock()
	c.Offset = offset
	c.Unlock()
}

func fetch(c *client.PartitionClient, offset int64) (*Response, error) {
	resp, err := c.Fetch(offset)
	if err != nil {
		return nil, err
	}
	return parseResponse(resp)
}

func (c *PartitionFetcher) Fetch() (*Response, error) {
	c.Lock()
	defer c.Unlock()
	resp, err := fetch(&(c.PartitionClient), c.Offset)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
