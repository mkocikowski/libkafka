// Package client contains base code for single partition producer and consumer implementations.
package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/ApiVersions"
	"github.com/mkocikowski/libkafka/api/CreateTopics"
	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/api/FindCoordinator"
	"github.com/mkocikowski/libkafka/api/ListOffsets"
	"github.com/mkocikowski/libkafka/api/Metadata"
	"github.com/mkocikowski/libkafka/api/OffsetCommit"
	"github.com/mkocikowski/libkafka/api/OffsetFetch"
	"github.com/mkocikowski/libkafka/api/Produce"
	"github.com/mkocikowski/libkafka/batch"
)

// LookupSrv and return unordered list of resolved host:port strings.
func LookupSrv(name string) ([]string, error) {
	_, srvs, err := net.LookupSRV("", "", name)
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, srv := range srvs {
		host := net.JoinHostPort(srv.Target, strconv.Itoa(int(srv.Port)))
		addrs = append(addrs, host)
	}
	return addrs, nil
}

// RandomBroker tries to resolve name through a call to LookupSrv. If
// successful it returns a random host:port from the list. If LookupSrv fails
// it returns name unmodified (so you can pass "localhost:9092" for example).
func RandomBroker(name string) string {
	addrs, err := LookupSrv(name)
	if err != nil {
		return name
	}
	if len(addrs) == 0 { // is this possible?
		return name
	}
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return addrs[0]
}

// TODO: timeouts
func call(conn io.ReadWriter, req *api.Request) (*api.Response, error) {
	out := bufio.NewWriter(conn)
	if _, err := out.Write(req.Bytes()); err != nil {
		return nil, err
	}
	if err := out.Flush(); err != nil {
		return nil, err
	}
	in := bufio.NewReader(conn)
	return api.Read(in)
}

func request(bootstrap string, req *api.Request, v interface{}) error {
	conn, err := net.Dial("tcp", RandomBroker(bootstrap))
	if err != nil {
		return fmt.Errorf("error connecting to broker: %v", err)
	}
	defer conn.Close()
	resp, err := call(conn, req)
	if err != nil {
		return fmt.Errorf("error making api call: %v", err)
	}
	if err := resp.Unmarshal(v); err != nil {
		return fmt.Errorf("error parsing api response: %v", err)
	}
	return nil
}

func PartitionLeaders(bootstrap, topic string) (map[int32]*Metadata.Broker, error) {
	v, err := GetMetadata(bootstrap, []string{topic})
	if err != nil {
		return nil, err
	}
	return v.Leaders(topic), nil
}

func CallFindCoordinator(bootstrap, group string) (*FindCoordinator.Response, error) {
	req := FindCoordinator.NewRequest(group)
	resp := &FindCoordinator.Response{}
	return resp, request(bootstrap, req, resp)
}

func CallOffsetFetch(bootstrap, group, topic string, partition int32) (*OffsetFetch.Response, error) {
	req := OffsetFetch.NewRequest(group, topic, partition)
	resp := &OffsetFetch.Response{}
	return resp, request(bootstrap, req, resp)
}

func CallOffsetCommit(bootstrap, group, topic string, partition int32, offset, retentionMs int64) (*OffsetCommit.Response, error) {
	req := OffsetCommit.NewRequest(group, topic, partition, offset, retentionMs)
	resp := &OffsetCommit.Response{}
	return resp, request(bootstrap, req, resp)
}

func GetApiVersions(bootstrap string) (*ApiVersions.Response, error) {
	req := ApiVersions.NewRequest()
	resp := &ApiVersions.Response{}
	return resp, request(bootstrap, req, resp)
}

func GetMetadata(bootstrap string, topics []string) (*Metadata.Response, error) {
	req := Metadata.NewRequest(topics)
	resp := &Metadata.Response{}
	return resp, request(bootstrap, req, resp)
}

func CreateTopic(bootstrap, topic string, numPartitions int32, replicationFactor int16) (*CreateTopics.Response, error) {
	req := CreateTopics.NewRequest(topic, numPartitions, replicationFactor, []CreateTopics.Config{})
	resp := &CreateTopics.Response{}
	return resp, request(bootstrap, req, resp)
}

/*
PartitionClient maintains a connection to the leader of a single topic partition.

The client uses the Bootstrap value to lookup topic metadata and to connect to
the Leader of given topic partition. This happens on the first API call.
Connections are persistent. All client API calls are synchronous. If an API
call can't complete the request-response round trip or if Kafka response can't
be parsed then the API call returns an error and the underlying connection is
closed (it will be re-opened on next call). If response is parsed successfully
no error is returned but this means only that the request-response round trip
was completed: there could be an error code returned in the Kafka response
itself. Checking for and interpreting that error is up to the user. Retries are
up to the user.

All PartitionClient calls are safe for concurrent use.
*/
type PartitionClient struct {
	sync.Mutex
	Bootstrap string // SRV or host:port used to contact the cluster. Does not need to be the leader of the topic partition.
	Topic     string
	Partition int32
	leader    *Metadata.Broker
	conn      net.Conn
}

func (c *PartitionClient) connect() error {
	if c.conn != nil {
		return nil
	}
	leaders, err := PartitionLeaders(c.Bootstrap, c.Topic)
	if err != nil {
		return fmt.Errorf("error getting partition leaders for %s: %v", c.Topic, err)
	}
	var ok bool
	if c.leader, ok = leaders[c.Partition]; !ok {
		return fmt.Errorf("can not find leader for %s:%d", c.Topic, c.Partition)
	}
	c.conn, err = net.DialTimeout("tcp", c.leader.Addr(), time.Second)
	if err != nil {
		return fmt.Errorf("error connecting to partition leader %+v: %v", c.leader, err)
	}
	return nil
}

func (c *PartitionClient) request(req *api.Request, v interface{}) error {
	c.Lock()
	defer c.Unlock()
	if err := c.connect(); err != nil {
		return err
	}
	resp, err := call(c.conn, req)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("error making api call to broker %+v: %v", c.leader, err)
	}
	if err := resp.Unmarshal(v); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("error parsing api response from broker %+v: %v", c.leader, err)
	}
	return nil
}

// Close the connection to the topic partition leader. Nop if no active
// connection. If there is a request in progress blocks until the request
// completes. API calls can be made after Close (new connection will be
// opened).
func (c *PartitionClient) Close() {
	c.Lock()
	defer c.Unlock()
	if c.conn == nil {
		return
	}
	c.conn.Close()
	c.conn = nil
}

func (c *PartitionClient) ListOffsets(offset int64) (*ListOffsets.Response, error) {
	req := ListOffsets.NewRequest(c.Topic, c.Partition, offset)
	resp := &ListOffsets.Response{}
	return resp, c.request(req, resp)
}

func (c *PartitionClient) Fetch(offset int64) (*Fetch.Response, error) {
	req := Fetch.NewRequest(c.Topic, c.Partition, int64(offset))
	resp := &Fetch.Response{}
	return resp, c.request(req, resp)
}

func (c *PartitionClient) Produce(b *batch.Batch) (*Produce.Response, error) {
	if b == nil {
		return nil, fmt.Errorf("no data")
	}
	buf := new(bytes.Buffer)
	buf.Write(b.Marshal())
	req := Produce.NewRequest(c.Topic, c.Partition, buf.Bytes())
	resp := &Produce.Response{}
	return resp, c.request(req, resp)
}
