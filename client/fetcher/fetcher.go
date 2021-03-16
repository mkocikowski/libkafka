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

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/api/Metadata"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
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
		Topic:          topicResponse.Topic,
		Partition:      partitionResponse.Partition,
		ThrottleTimeMs: r.ThrottleTimeMs,
		ErrorCode:      partitionResponse.ErrorCode,
		LogStartOffset: partitionResponse.LogStartOffset,
		HighWatermark:  partitionResponse.HighWatermark,
		RecordSet:      batch.RecordSet(partitionResponse.RecordSet),
	}, nil
}

type Response struct {
	Broker         *Metadata.Broker
	Topic          string
	Partition      int32
	ThrottleTimeMs int32
	ErrorCode      int16
	LogStartOffset int64
	HighWatermark  int64
	RecordSet      batch.RecordSet `json:"-"`
}

type PartitionFetcher struct {
	sync.Mutex
	client.PartitionClient
	offset int64
	// The minimum amount of data the server should return for a fetch
	// request. If insufficient data is available the request will wait for
	// that much data to accumulate before answering the request (up to
	// limit set by MaxWaitTimeMs).
	MinBytes int32
	MaxBytes int32
	// The maximum amount of time the server will block before answering
	// the fetch request if there isn't sufficient data to immediately
	// satisfy the requirement given by MinBytes. Keep it < libkafka.RequestTimeout.
	MaxWaitTimeMs int32
}

var (
	MessageNewest = time.Unix(0, -1e6)
	MessageOldest = time.Unix(0, -2e6)
)

// Seek looks up an offset close to specified timestamp and sets the fetcher's
// offset to it. If there is any error the fetcher's offset is not modified.
// MessageNewest and MessageOldest are two "magic" values for the target.
func (c *PartitionFetcher) Seek(target time.Time) error {
	c.Lock()
	defer c.Unlock()
	timestampMs := target.UnixNano() / int64(time.Millisecond)
	resp, err := c.PartitionClient.ListOffsets(timestampMs)
	if err != nil {
		return err
	}
	p := resp.Responses[0].Partitions[0]
	if p.ErrorCode != libkafka.ERR_NONE {
		return &libkafka.Error{Code: p.ErrorCode}
	}
	c.offset = p.Offset
	return nil
}

func (c *PartitionFetcher) Offset() int64 {
	c.Lock()
	defer c.Unlock()
	return c.offset
}

func (c *PartitionFetcher) SetOffset(offset int64) {
	c.Lock()
	c.offset = offset
	c.Unlock()
}

func fetch(c *client.PartitionClient, args *Fetch.Args) (*Response, error) {
	resp, err := c.Fetch(args)
	if err != nil {
		return nil, err
	}
	return parseResponse(resp)
}

func (c *PartitionFetcher) Fetch() (*Response, error) {
	c.Lock()
	defer c.Unlock()
	args := &Fetch.Args{
		ClientId:      c.ClientId,
		Topic:         c.Topic,
		Partition:     c.Partition,
		Offset:        c.offset,
		MinBytes:      c.MinBytes,
		MaxBytes:      c.MaxBytes,
		MaxWaitTimeMs: c.MaxWaitTimeMs,
	}
	resp, err := fetch(&(c.PartitionClient), args)
	if err != nil {
		if leader := c.Leader(); leader != nil {
			err = fmt.Errorf("error calling %+v: %w", leader, err)
		}
		return nil, err
	}
	resp.Broker = c.Leader()
	return resp, nil
}
