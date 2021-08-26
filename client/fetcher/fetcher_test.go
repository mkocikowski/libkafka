package fetcher

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/batch"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntergationPartitionFetcher(t *testing.T) {
	bootstrap := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, nil, topic, 1, 1); err != nil {
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
	if s := resp.Broker.String(); s != ":0:localhost:9092" {
		t.Fatal(s)
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
	if resp.ErrorCode != libkafka.ERR_NONE {
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
	if resp.ErrorCode != libkafka.ERR_OFFSET_OUT_OF_RANGE {
		t.Fatalf("%+v", resp)
	}
	//
	c.offset = -1
	resp, _ = c.Fetch()
	if resp.ErrorCode != libkafka.ERR_OFFSET_OUT_OF_RANGE {
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
	if resp.ErrorCode != libkafka.ERR_NONE {
		t.Fatalf("%+v", resp)
	}
	batches = resp.RecordSet.Batches()
	if len(batches) != 0 {
		t.Fatalf("%+v", resp)
	}
}

// test that when fetching the newest offset (where the offset is the same as
// the high watermark), if there are no new records ready to be fetched, then
// there is no error, and no error code
func TestIntergationPartitionFetcherEmptyPartition(t *testing.T) {
	bootstrap := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, nil, topic, 1, 1); err != nil {
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
	if resp.ErrorCode != libkafka.ERR_NONE {
		log.Fatalf("%+v", resp)
	}
	if len(resp.RecordSet) != 0 {
		log.Fatalf("%+v", resp)
	}
}
