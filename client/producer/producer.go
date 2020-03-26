// Package producer implements a single partition Kafka producer.
package producer

import (
	"fmt"
	"time"

	"github.com/mkocikowski/libkafka/api/Produce"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/compression"
)

func parseResponse(r *Produce.Response) (*Response, error) {
	if n := len(r.TopicResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of topic responses: %d", n)
	}
	tr := &(r.TopicResponses[0])
	if n := len(tr.PartitionResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of partition responses: %d", n)
	}
	pr := &(tr.PartitionResponses[0])
	return &Response{
		ThrottleTimeMs: r.ThrottleTimeMs,
		Topic:          tr.Topic,
		Partition:      pr.Partition,
		ErrorCode:      pr.ErrorCode,
		BaseOffset:     pr.BaseOffset,
		LogAppendTime:  pr.LogAppendTime,
		LogStartOffset: pr.LogStartOffset,
	}, nil
}

type Response struct {
	Topic          string
	Partition      int32
	ThrottleTimeMs int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64
	LogStartOffset int64
}

type PartitionProducer struct {
	client.PartitionClient
}

// ProduceStrings with Nop compression.
func (p *PartitionProducer) ProduceStrings(now time.Time, values ...string) (*Response, error) {
	b, err := batch.NewBuilder(now).AddStrings(values...).Build(now, &compression.Nop{})
	if err != nil {
		return nil, err
	}
	return p.Produce(b)
}

// Produce (send) batch to Kafka. Single request is made (no retries). The call
// is blocking. See documentation for client.PartitionClient for details on how
// errors are handled.
func (p *PartitionProducer) Produce(b *batch.Batch) (*Response, error) {
	resp, err := p.PartitionClient.Produce(b)
	if err != nil {
		return nil, err
	}
	return parseResponse(resp)
}
