// +build v1_0

package producer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/client"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestIntergationPartitionProducer_v1_0(t *testing.T) {
	bootstrap := "localhost:9093"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	if _, err := client.CallCreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	p := &PartitionProducer{
		PartitionClient: client.PartitionClient{
			Bootstrap: bootstrap,
			Topic:     topic,
			Partition: 0,
		},
		Acks:      1,
		TimeoutMs: 1000,
	}
	resp, err := p.ProduceStrings(time.Now(), "foo", "bar")
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.ErrorCode; code != libkafka.ERR_NONE {
		t.Fatal(&libkafka.Error{Code: code})
	}
	t.Logf("%+v", resp)
}
