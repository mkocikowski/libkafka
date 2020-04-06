// Package consumer implements a single partition Kafka consumer.
package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/errors"
)

func unmarshal(recordSetBytes []byte) ([]*batch.Batch, error) {
	var unmarshaledBatches []*batch.Batch
	for _, recordBatchBytes := range batch.RecordSet(recordSetBytes).Unmarshal() {
		b, err := batch.Unmarshal(recordBatchBytes)
		if err != nil {
			return nil, err
		}
		unmarshaledBatches = append(unmarshaledBatches, b)
	}
	return unmarshaledBatches, nil
}

func parseResponse(r *Fetch.Response) (*Response, error) {
	if n := len(r.TopicResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of topic responses: %d", n)
	}
	topicResponse := &(r.TopicResponses[0])
	if n := len(topicResponse.PartitionResponses); n != 1 {
		return nil, fmt.Errorf("unexpected number of partition responses: %d", n)
	}
	partitionResponse := &(topicResponse.PartitionResponses[0])
	batches, err := unmarshal(partitionResponse.RecordSet)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling batches: %v", err)
	}
	numRecords := 0
	for _, b := range batches {
		b.Topic = topicResponse.Topic
		b.Partition = partitionResponse.Partition
		numRecords += int(b.NumRecords)
	}
	return &Response{
		ThrottleTimeMs:     r.ThrottleTimeMs,
		Topic:              topicResponse.Topic,
		Partition:          partitionResponse.Partition,
		ErrorCode:          partitionResponse.ErrorCode,
		LogStartOffset:     partitionResponse.LogStartOffset,
		HighWatermark:      partitionResponse.HighWatermark,
		RecordBatches:      batches,
		RecordSetSizeBytes: len(partitionResponse.RecordSet),
		NumRecords:         numRecords,
	}, err
}

type Response struct {
	Topic              string
	Partition          int32
	ThrottleTimeMs     int32
	ErrorCode          int16
	LogStartOffset     int64
	HighWatermark      int64
	RecordBatches      []*batch.Batch
	RecordSetSizeBytes int
	NumRecords         int
}

/*
func (resp *Response) recordsBytes() ([][]byte, error) {
	var recordsBytes [][]byte
	for _, batch := range resp.RecordBatches {
		r, err := batch.Records(&compression.Nop{}) // TODO
		if err != nil {
			return nil, err
		}
		recordsBytes = append(recordsBytes, r...)
	}
	return recordsBytes, nil
}

func (resp *Response) Records() ([]*record.Record, error) {
	recordsBytes, err := resp.recordsBytes()
	if err != nil {
		return nil, err
	}
	var records []*record.Record
	for _, b := range recordsBytes {
		resp.RecordsBytes += len(b)
		r, err := record.Unmarshal(b)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling record: %v", err)
		}
		records = append(records, r)
	}
	return records, nil
}
*/
type PartitionConsumer struct {
	sync.Mutex
	client.PartitionClient
	Offset int64
}

var (
	MessageNewest = time.Unix(0, -1e6)
	MessageOldest = time.Unix(0, -2e6)
)

func (c *PartitionConsumer) Seek(offset time.Time) error {
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

func fetch(c *client.PartitionClient, offset int64) (*Response, error) {
	resp, err := c.Fetch(offset)
	if err != nil {
		return nil, err
	}
	return parseResponse(resp)
}

func (c *PartitionConsumer) Fetch() (*Response, error) {
	c.Lock()
	defer c.Unlock()
	resp, err := fetch(&(c.PartitionClient), c.Offset)
	if err != nil {
		return nil, err
	}
	for _, batch := range resp.RecordBatches {
		c.Offset = batch.BaseOffset + int64(batch.LastOffsetDelta) + 1
	}
	return resp, nil
}
