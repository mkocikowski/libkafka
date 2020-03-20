package consumer

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/client/producer"
	"github.com/mkocikowski/libkafka/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntergationPartitionConsumer(t *testing.T) {
	bootstrap := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	p := &producer.PartitionProducer{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
	}
	if _, err := p.ProduceStrings(time.Now(), "foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ProduceStrings(time.Now(), "monkey", "banana"); err != nil {
		t.Fatal(err)
	}
	//
	c := &PartitionConsumer{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
	}
	resp, err := c.Consume()
	if err != nil {
		log.Fatal(err)
	}
	highWatermark := resp.HighWatermark
	if highWatermark != 4 {
		t.Fatalf("%+v", resp)
	}
	if c.Offset != 4 {
		t.Fatalf("%+v", c)
	}
	if len(resp.RecordBatches) != 2 {
		t.Fatalf("%+v", resp)
	}
	batch := resp.RecordBatches[1]
	if batch.BaseOffset != 2 {
		t.Fatalf("%+v", batch)
	}
	if batch.LastOffsetDelta != 1 {
		t.Fatalf("%+v", batch)
	}
	//
	resp, err = c.Consume()
	if err != nil {
		log.Fatal(err)
	}
	if len(resp.RecordBatches) != 0 {
		t.Fatalf("%+v", resp)
	}
	if resp.ErrorCode != errors.NONE {
		t.Fatalf("%+v", resp)
	}
	//
	if _, err := p.ProduceStrings(time.Now(), "hello"); err != nil {
		t.Fatal(err)
	}
	resp, err = c.Consume()
	if err != nil {
		log.Fatal(err)
	}
	if len(resp.RecordBatches) != 1 {
		t.Fatalf("%+v", resp)
	}
	if c.Offset != 5 {
		t.Fatalf("%+v", c)
	}
}
