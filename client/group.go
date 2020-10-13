package client

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/FindCoordinator"
	"github.com/mkocikowski/libkafka/api/Heartbeat"
	"github.com/mkocikowski/libkafka/api/JoinGroup"
	"github.com/mkocikowski/libkafka/api/OffsetCommit"
	"github.com/mkocikowski/libkafka/api/OffsetFetch"
	"github.com/mkocikowski/libkafka/api/SyncGroup"
)

func CallFindCoordinator(bootstrap, groupId string) (*FindCoordinator.Response, error) {
	req := FindCoordinator.NewRequest(groupId)
	resp := &FindCoordinator.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, req, resp)
}

func GetGroupCoordinator(bootstrap, groupId string) (string, error) {
	resp, err := CallFindCoordinator(bootstrap, groupId)
	if err != nil {
		return "", fmt.Errorf("error making FindCoordinator call: %w", err)
	}
	if resp.ErrorCode != 0 {
		return "", fmt.Errorf("error response from FindCoordinator call: %w", &libkafka.Error{Code: resp.ErrorCode})
	}
	return net.JoinHostPort(resp.Host, strconv.Itoa(int(resp.Port))), nil
}

// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal

type GroupClient struct {
	sync.Mutex
	Bootstrap string
	GroupId   string
	conn      net.Conn
}

func (c *GroupClient) connect() error {
	if c.conn != nil {
		return nil
	}
	addr, err := GetGroupCoordinator(c.Bootstrap, c.GroupId)
	if err != nil {
		return err
	}
	c.conn, err = net.DialTimeout("tcp", addr, libkafka.DialTimeout)
	if err != nil {
		return fmt.Errorf("error connecting to group coordinator: %w", err)
	}
	return nil
}

func (c *GroupClient) disconnect() error {
	if c.conn == nil {
		return nil
	}
	c.conn.Close()
	c.conn = nil
	return nil
}

// Close the connection to the group coordinator. Nop if no active connection.
// If there is a request in progress blocks until the request completes. This,
// like its counterpart in PartitionClient, is intended to give higher level
// libraries ability to respond to errors by closing the connection (and so
// forcing reconnect on next call).
func (c *GroupClient) Close() error { // implement io.Closer
	c.Lock()
	defer c.Unlock()
	c.disconnect()
	return nil
}

// Call makes a request (connecting to the coordinator if necessary) and reads
// the response. If there is error making the request or reading the response,
// it disconnects. Response is not interpreted (ie, Call does not look at the
// possible error codes inside the kafka response). The purpose of this method
// is to allow users to make their "own" requests - using different api
// version, or calling for multiple topic-partitions. For use, see the source
// of methods such as FetchOffset (basically, you pass it a struct with the
// request that will be marshaled into wire format, and a struct pointer into
// which response will be unmarshaled: these structs are defined in the api
// package for various api keys, but you can provide your own). This is a
// low-level method that was private; I decided to make it public to give users
// more flexibility. We'll see how it goes.
func (c *GroupClient) Call(req *api.Request, respStructPtr interface{}) error {
	c.Lock()
	defer c.Unlock()
	if err := c.connect(); err != nil {
		return err
	}
	err := call(c.conn, req, respStructPtr)
	if err != nil {
		c.disconnect()
	}
	return err
}

func (c *GroupClient) callJoin(memberId, protoType string, protocols []JoinGroup.Protocol) (*JoinGroup.Response, error) {
	req := JoinGroup.NewRequest(c.GroupId, memberId, protoType, protocols)
	resp := &JoinGroup.Response{}
	return resp, c.Call(req, resp)
}

func (c *GroupClient) callSync(memberId string, generationId int32, assignments []SyncGroup.Assignment) (*SyncGroup.Response, error) {
	req := SyncGroup.NewRequest(c.GroupId, memberId, generationId, assignments)
	//log.Printf("%+v", req)
	resp := &SyncGroup.Response{}
	return resp, c.Call(req, resp)
}

type JoinGroupRequest struct {
	MemberId     string
	ProtocolType string
	ProtocolName string
	Metadata     []byte
	//group.initial.rebalance.delay.ms
}

func (c *GroupClient) Join(req *JoinGroupRequest) (*JoinGroup.Response, error) {
	p := JoinGroup.Protocol{
		Name:     req.ProtocolName,
		Metadata: req.Metadata,
	}
	return c.callJoin(req.MemberId, req.ProtocolType, []JoinGroup.Protocol{p})
}

type SyncGroupRequest struct {
	MemberId     string
	GenerationId int32
	Assignments  []SyncGroup.Assignment
}

func (c *GroupClient) Sync(req *SyncGroupRequest) (*SyncGroup.Response, error) {
	return c.callSync(req.MemberId, req.GenerationId, req.Assignments)
}

func (c *GroupClient) Heartbeat(memberId string, generationId int32) (*Heartbeat.Response, error) {
	req := Heartbeat.NewRequest(c.GroupId, memberId, generationId)
	resp := &Heartbeat.Response{}
	return resp, c.Call(req, resp)
}

func parseOffsetFetchResponse(r *OffsetFetch.Response) (int64, error) {
	if r.ErrorCode != libkafka.ERR_NONE {
		return -1, &libkafka.Error{Code: r.ErrorCode}
	}
	if n := len(r.Topics); n != 1 {
		return -1, fmt.Errorf("unexpected number of topic responses: %d", n)
	}
	t := r.Topics[0]
	if n := len(t.Partitions); n != 1 {
		return -1, fmt.Errorf("unexpected number of topic partition responses: %d", n)
	}
	p := t.Partitions[0]
	if p.ErrorCode != libkafka.ERR_NONE {
		return -1, &libkafka.Error{Code: p.ErrorCode}
	}
	return p.CommitedOffset, nil
}

// Fetch last commited offset for topic partition. If the topic partition does
// not exist, or there is no offset commited for it, returns -1 and no error.
func (c *GroupClient) FetchOffset(topic string, partition int32) (int64, error) {
	req := OffsetFetch.NewRequest(c.GroupId, topic, partition)
	resp := &OffsetFetch.Response{}
	if err := c.Call(req, resp); err != nil {
		return -1, fmt.Errorf("error making fetch offsets call: %w", err)
	}
	return parseOffsetFetchResponse(resp)
}

func parseOffsetCommitResponse(r *OffsetCommit.Response) error {
	if n := len(r.Topics); n != 1 {
		return fmt.Errorf("unexpected number of topic responses: %d", n)
	}
	t := r.Topics[0]
	if n := len(t.Partitions); n != 1 {
		return fmt.Errorf("unexpected number of topic partition responses: %d", n)
	}
	p := t.Partitions[0]
	if p.ErrorCode != libkafka.ERR_NONE {
		return &libkafka.Error{Code: p.ErrorCode}
	}
	return nil
}

func (c *GroupClient) CommitOffset(topic string, partition int32, offset, retentionMs int64) error {
	req := OffsetCommit.NewRequest(c.GroupId, topic, partition, offset, retentionMs)
	resp := &OffsetCommit.Response{}
	if err := c.Call(req, resp); err != nil {
		return fmt.Errorf("error making commit offsets call: %w", err)
	}
	return parseOffsetCommitResponse(resp)
}
