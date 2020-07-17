package client

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api/SyncGroup"
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
		if resp.ErrorCode != libkafka.ERR_NONE {
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
	if resp.ErrorCode != libkafka.ERR_NONE {
		t.Fatalf("%+v", resp)
	}
	if string(resp.Assignment) != "foo" {
		t.Fatalf("%+v", resp)
	}
	for i := 0; i < 10; i++ {
		if resp, _ := c.Heartbeat(memberId, generationId); resp.ErrorCode != libkafka.ERR_NONE {
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
		if resp != nil && resp.ErrorCode == libkafka.ERR_UNKNOWN_MEMBER_ID {
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
	multiPartitionedTopic := fmt.Sprintf("test-multi-%x", rand.Uint32())
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
	if err.(*libkafka.Error).Code != libkafka.ERR_UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatal(err)
	}
	//
	if _, err = CallCreateTopic(bootstrap, nil, topic, 1, 1); err != nil {
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

	// CommitMultiplePartitionsOffsets
	offsets := map[int32]int64{
		0: 10,
	}
	// CommitMultiplePartitionsOffsets to a single-partition topic should be fine
	if err = c.CommitMultiplePartitionsOffsets(topic, offsets, 1000); err != nil {
		t.Fatal(err)
	}
	offset, err = c.FetchOffset(topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 10 {
		t.Fatal(offset)
	}
	offsets = map[int32]int64{
		0: 100,
		1: 200,
	}
	// CommitMultiplePartitionsOffsets with two partitions to a single-partition topic should
	// return UNKNOWN_TOPIC_OR_PARTITION error
	err = c.CommitMultiplePartitionsOffsets(topic, offsets, 1000)
	if err.(*libkafka.Error).Code != libkafka.ERR_UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("we didn't get expected UNKNOWN_TOPIC_OR_PARTITION error, got instead: %v", err)
	}
	if _, err = CallCreateTopic(bootstrap, nil, multiPartitionedTopic, 2, 1); err != nil {
		t.Fatalf("we got unexpected error during creating multi-partitioned topic: %v", err)
	}
	// CommitOfCommitMultiplePartitionsOffsetsfsets with two partitions to a multi-partitioned topic should be
	// fine
	if err = c.CommitMultiplePartitionsOffsets(multiPartitionedTopic, offsets, 1000); err != nil {
		t.Fatalf("we got unexpected error commit offsets: %v", err)
	}

	offset, err = c.FetchOffset(multiPartitionedTopic, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 100 {
		t.Fatal(offset)
	}
	offset, err = c.FetchOffset(multiPartitionedTopic, 1)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 200 {
		t.Fatal(offset)
	}
}

func TestIntegrationGroupOffsetsTLS(t *testing.T) {
	bootstrap := "localhost:9093"
	c := &GroupClient{
		Bootstrap: bootstrap,
		TLS:       mTLSConfig(),
		GroupId:   fmt.Sprintf("test-group-%x", rand.Uint32()),
	}
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	// topic doesn't exist and there is no offset commited
	if _, err := c.FetchOffset(topic, 0); err != nil {
		t.Fatal(err)
	}
}
