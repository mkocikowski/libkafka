package client

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api/CreateTopics"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntegrationCallApiVersions(t *testing.T) {
	r, err := CallApiVersions("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationCallApiVersionsBadHost(t *testing.T) {
	_, err := CallApiVersions("foo")
	if err == nil {
		t.Fatal("expected bad host error")
	}
	t.Log(err)
}

func TestIntegrationCallCreateTopic(t *testing.T) {
	brokers := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	var r *CreateTopics.Response
	r, _ = CallCreateTopic(brokers, topic, 1, 2)
	if r.Topics[0].ErrorCode != libkafka.ERR_INVALID_REPLICATION_FACTOR {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	r, _ = CallCreateTopic(brokers, topic, 1, 1)
	if r.Topics[0].ErrorCode != libkafka.ERR_NONE {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	r, _ = CallCreateTopic(brokers, topic, 1, 1)
	if r.Topics[0].ErrorCode != libkafka.ERR_TOPIC_ALREADY_EXISTS {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	if _, err := CallCreateTopic("none:9092", topic, 1, 1); err == nil {
		t.Fatal("expected error")
	}
}
