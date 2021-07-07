package client

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
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

func TestIntegrationCallCreateTopicRequestTimeout(t *testing.T) {
	d := libkafka.RequestTimeout
	defer func() {
		libkafka.RequestTimeout = d
	}()
	libkafka.RequestTimeout = time.Nanosecond
	brokers := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	_, err := CallCreateTopic(brokers, topic, 1, 2)
	for {
		err := errors.Unwrap(err)
		if err == nil {
			break
		}
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return // success
		}
	}
	t.Fatalf("expected timeout got %v", err)
}

func TestUnitConnectToRandomBrokerAndCallErrorForgetSRV(t *testing.T) {
	srvLookupCache["foo"] = []string{"bar:1"}
	err := connectToRandomBrokerAndCall("foo", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := srvLookupCache["foo"]; ok {
		t.Fatal("expected key to be deleted because of call error")
	}
}
