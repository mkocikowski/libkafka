package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/api/ListOffsets"
	"github.com/mkocikowski/libkafka/api/Metadata"
	"github.com/mkocikowski/libkafka/api/Produce"
)

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
itself. Checking for and interpreting that error (and possibly closing the
connection) is up to the user. Retries are up to the user.

All PartitionClient calls are safe for concurrent use.
*/
type PartitionClient struct {
	sync.Mutex
	Bootstrap string // srv or host:port
	Topic     string
	Partition int32
	leader    *Metadata.Broker
	conn      net.Conn
}

func (c *PartitionClient) connect() error {
	if c.conn != nil {
		return nil
	}
	leaders, err := GetPartitionLeaders(c.Bootstrap, c.Topic)
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
		return fmt.Errorf("error parsing api %T response from broker %+v: %v",
			req.Body, c.leader, err)
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

func (c *PartitionClient) Produce(batch []byte) (*Produce.Response, error) {
	req := Produce.NewRequest(c.Topic, c.Partition, batch)
	resp := &Produce.Response{}
	return resp, c.request(req, resp)
}
