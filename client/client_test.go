package client

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/api/CreateTopics"
	"github.com/mkocikowski/libkafka/errors"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntegrationGetApiVersions(t *testing.T) {
	r, err := GetApiVersions("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationGetApiVersionsBadHost(t *testing.T) {
	_, err := GetApiVersions("foo")
	if err == nil {
		t.Fatal("expected bad host error")
	}
	t.Log(err)
}

func TestIntegrationCallFindCoordinator(t *testing.T) {
	brokers := "localhost:9092"
	topic := "test"
	group := "test-group"
	if _, err := CreateTopic(brokers, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	r, err := CallFindCoordinator(brokers, group)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationCallOffsetFetch(t *testing.T) {
	brokers := "localhost:9092"
	topic := "test"
	group := "test-group"
	if _, err := CreateTopic(brokers, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	r, err := CallOffsetFetch(brokers, group, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationCallOffsetCommit(t *testing.T) {
	brokers := "localhost:9092"
	topic := "test"
	group := "test-group"
	if _, err := CreateTopic(brokers, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	r, err := CallOffsetCommit(brokers, group, topic, 0, 10, 5000)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationCreateTopic(t *testing.T) {
	brokers := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	var r *CreateTopics.Response
	r, _ = CreateTopic(brokers, topic, 1, 2)
	if r.Topics[0].ErrorCode != errors.INVALID_REPLICATION_FACTOR {
		t.Fatal(errors.Descriptions[int(r.Topics[0].ErrorCode)])
	}
	r, _ = CreateTopic(brokers, topic, 1, 1)
	if r.Topics[0].ErrorCode != errors.NONE {
		t.Fatal(errors.Descriptions[int(r.Topics[0].ErrorCode)])
	}
	r, _ = CreateTopic(brokers, topic, 1, 1)
	if r.Topics[0].ErrorCode != errors.TOPIC_ALREADY_EXISTS {
		t.Fatal(errors.Descriptions[int(r.Topics[0].ErrorCode)])
	}
	if _, err := CreateTopic("none:9092", topic, 1, 1); err == nil {
		t.Fatal("expected error")
	}
}
