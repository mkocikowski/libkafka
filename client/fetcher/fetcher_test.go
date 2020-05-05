package fetcher

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestUnitPartitionFetcherImplementsFetcher(t *testing.T) {
	c := &PartitionFetcher{}
	_ = Fetcher(c) // panics if does not implement
}

func TestIntergationPartitionFetcher(t *testing.T) {
	bootstrap := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	p := &producer.PartitionProducer{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
		Acks:      1,
		TimeoutMs: 1000,
	}
	if _, err := p.ProduceStrings(time.Now(), "foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceStrings(time.Now(), "monkey", "banana"); err != nil {
		t.Fatal(err)
	}
	//
	c := &PartitionFetcher{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
		MinBytes:      10 << 10,
		MaxBytes:      10 << 20,
		MaxWaitTimeMs: 1000,
	}
	resp, err := c.Fetch()
	if err != nil {
		log.Fatal(err)
	}
	highWatermark := resp.HighWatermark
	if highWatermark != 4 {
		t.Fatalf("%+v", resp)
	}
	if c.offset != 0 { // offset is not advanced automatically
		t.Fatalf("%+v", c)
	}
	batches := resp.RecordSet.Batches()
	if len(batches) != 2 {
		t.Fatalf("%+v", resp)
	}
	b, err := batch.Unmarshal(batches[1])
	if err != nil {
		t.Fatal(err)
	}
	if b.BaseOffset != 2 {
		t.Fatalf("%+v", b)
	}
	if b.LastOffsetDelta != 1 {
		t.Fatalf("%+v", b)
	}
	//
	c.offset = 4
	resp, err = c.Fetch()
	if err != nil {
		log.Fatal(err)
	}
	batches = resp.RecordSet.Batches()
	if len(batches) != 0 {
		t.Fatalf("%+v", resp)
	}
	if resp.ErrorCode != errors.NONE {
		t.Fatalf("%+v", resp)
	}
	//
	if _, err := p.ProduceStrings(time.Now(), "hello"); err != nil {
		t.Fatal(err)
	}
	resp, err = c.Fetch()
	if err != nil {
		log.Fatal(err)
	}
	batches = resp.RecordSet.Batches()
	if len(batches) != 1 {
		t.Fatalf("%+v", resp)
	}
	//
	c.offset = 10
	resp, _ = c.Fetch()
	if resp.ErrorCode != errors.OFFSET_OUT_OF_RANGE {
		t.Fatalf("%+v", resp)
	}
	//
	if err := c.Seek(MessageNewest); err != nil {
		t.Fatal(err)
	}
	if c.offset != 5 {
		t.Fatalf("%+v", c)
	}
	resp, _ = c.Fetch()
	if resp.ErrorCode != errors.NONE {
		t.Fatalf("%+v", resp)
	}
	batches = resp.RecordSet.Batches()
	if len(batches) != 0 {
		t.Fatalf("%+v", resp)
	}
}
