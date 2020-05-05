package client

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka/api/SyncGroup"
	"github.com/mkocikowski/libkafka/errors"
)

func TestIntegrationGroupClientJoin(t *testing.T) {
	c := &GroupClient{
		Bootstrap: "localhost:9092",
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	req := &JoinGroupRequest{
		ProtocolType: "partition",
		ProtocolName: "random",
		Metadata:     []byte{},
	}
	for i := 0; i < 10; i++ { // retry in case kafka not ready in travis
		resp, err := c.Join(req)
		if err != nil || resp.MemberId == "" {
			time.Sleep(time.Second)
			continue
		}
		if resp.ErrorCode != errors.NONE {
			t.Fatalf("%+v", resp)
		}
		if resp.GenerationId != 1 {
			t.Fatalf("%+v", resp)
		}
		return // success
	}
	t.Fatal()
}

func TestIntegrationGroupClientSyncAndHeartbeat(t *testing.T) {
	c := &GroupClient{
		Bootstrap: "localhost:9092",
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	var memberId string
	var generationId int32

	for i := 0; i < 10; i++ { // retry in case kafka not ready in travis
		req := &JoinGroupRequest{
			ProtocolType: "partition",
			ProtocolName: "random",
			Metadata:     []byte{},
		}
		resp, err := c.Join(req)
		if err == nil && resp.MemberId != "" {
			memberId = resp.MemberId
			generationId = resp.GenerationId
			break
		}
		time.Sleep(time.Second)
	}
	req := &SyncGroupRequest{
		MemberId:     memberId,
		GenerationId: generationId,
		Assignments: []SyncGroup.Assignment{
			SyncGroup.Assignment{
				MemberId:   memberId,
				Assignment: []byte("foo"),
			},
		},
	}
	resp, _ := c.Sync(req)
	if resp.ErrorCode != errors.NONE {
		t.Fatalf("%+v", resp)
	}
	if string(resp.Assignment) != "foo" {
		t.Fatalf("%+v", resp)
	}
	for i := 0; i < 10; i++ {
		if resp, _ := c.Heartbeat(memberId, generationId); resp.ErrorCode != errors.NONE {
			t.Fatalf("%+v", resp)
		}
	}
}

func TestIntegrationGroupClientSyncUnknownMemberId(t *testing.T) {
	c := &GroupClient{
		Bootstrap: "localhost:9092",
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	for i := 0; i < 10; i++ { // retry in case kafka not ready in travis
		req := &SyncGroupRequest{Assignments: []SyncGroup.Assignment{}}
		resp, _ := c.Sync(req)
		if resp != nil && resp.ErrorCode == errors.UNKNOWN_MEMBER_ID {
			return // success
		}
		time.Sleep(time.Second)
	}
	t.Fatal()
}

func TestIntegrationGroupOffsets(t *testing.T) {
	bootstrap := "localhost:9092"
	c := &GroupClient{
		Bootstrap: bootstrap,
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	var offset int64
	var err error
	// topic doesn't exist and there is no offset commited
	offset, err = c.FetchOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != -1 {
		t.Fatal(offset)
	}
	// try commiting offset for topic that doesn't exist
	err = c.CommitOffset(topic, 0, 1, 1000)
	if err.(*errors.KafkaError).Code != errors.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(err)
	}
	//
	if _, err = CallCreateTopic(bootstrap, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	// get offset for existing topic but where no offset has been committed
	offset, err = c.FetchOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != -1 {
		t.Fatal(offset)
	}
	//
	if err = c.CommitOffset(topic, 0, 1, 1000); err != nil {
		t.Fatal(err)
	}
	//
	offset, err = c.FetchOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 1 {
		t.Fatal(offset)
	}
}
