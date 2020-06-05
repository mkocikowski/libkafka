package client

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/api/Metadata"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntergationPartitionClientBadBootstrap(t *testing.T) {
	bootstrap := "foo"
	topic := fmt.Sprintf("test-%x", rand.Uint32()) // do not create
	c := &PartitionClient{
		Bootstrap: bootstrap,
		Topic:     topic,
		Partition: 0,
	}
	_, err := c.ListOffsets(0)
	if err == nil {
		t.Fatal("expected 'dial tcp' error")
	}
	t.Log(err)
}

func TestIntergationPartitionClientTopicDoesNotExist(t *testing.T) {
	bootstrap := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32()) // do not create
	c := &PartitionClient{
		Bootstrap: bootstrap,
		Topic:     topic,
		Partition: 0,
	}
	_, err := c.ListOffsets(0)
	if !errors.Is(err, ErrPartitionDoesNotExist) {
		t.Fatal(err)
	}
	t.Log(err)
}

func TestUnitLeaderString(t *testing.T) {
	b := &Metadata.Broker{Rack: "foo", NodeId: 1, Host: "bar", Port: 9092}
	s := fmt.Sprintf("%v", b)
	if s != "foo:1:bar:9092" {
		t.Fatal(s)
	}
}
