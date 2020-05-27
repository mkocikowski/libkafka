package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/ApiVersions"
	"github.com/mkocikowski/libkafka/api/Fetch"
	"github.com/mkocikowski/libkafka/api/ListOffsets"
	"github.com/mkocikowski/libkafka/api/Metadata"
	"github.com/mkocikowski/libkafka/api/Produce"
)

func GetPartitionLeaders(bootstrap, topic string) (map[int32]*Metadata.Broker, error) {
	meta, err := CallMetadata(bootstrap, []string{topic})
	if err != nil {
		return nil, err
	}
	return meta.Leaders(topic), nil
}

// PartitionClient maintains a connection to the leader of a single topic
// partition. The client uses the Bootstrap value to look up topic metadata and
// to connect to the Leader of given topic partition. This happens on the first
// API call. Connections are persistent. All client API calls are synchronous.
// If an API call can't complete the request-response round trip or if Kafka
// response can't be parsed then the API call returns an error and the
// underlying connection is closed (it will be re-opened on next call). If
// response is parsed successfully no error is returned but this means only
// that the request-response round trip was completed: there could be an error
// code returned in the Kafka response itself. Checking for and interpreting
// that error (and possibly calling Close) is up to the user. Retries are up to
// the user. All PartitionClient calls are safe for concurrent use.
type PartitionClient struct {
	sync.Mutex
	Bootstrap string // srv or host:port
	ClientId  string
	Topic     string
	Partition int32
	leader    *Metadata.Broker
	versions  *ApiVersions.Response
	conn      net.Conn
}

// find partition leader, connect to it, and set c.leader
func (c *PartitionClient) connect() (err error) {
	if c.conn != nil {
		return nil
	}
	leaders, err := GetPartitionLeaders(c.Bootstrap, c.Topic)
	if err != nil {
		return fmt.Errorf("error getting topic %v partition %d leader: %w", c.Topic, c.Partition, err)
	}
	c.leader = leaders[c.Partition]
	if c.leader == nil {
		return fmt.Errorf("no leader for %v partition %d", c.Topic, c.Partition)
	}
	c.conn, err = net.DialTimeout("tcp", c.leader.Addr(), time.Second)
	if err != nil {
		return fmt.Errorf("error connecting to topic %v partition %d leader %v: %w", c.Topic, c.Partition, c.leader, err)
	}
	// version information is needed only for the kafka 1.0 produce hack
	c.versions, err = apiVersions(c.conn)
	if err != nil {
		return fmt.Errorf("error getting api versions from broker %v: %w", c.leader, err)
	}
	if code := c.versions.ErrorCode; code != libkafka.ERR_NONE {
		return fmt.Errorf("error response for api versions call from broker %v: %w", c.leader, libkafka.Error{Code: code})
	}
	return nil
}

// close connection to leader, but do not zero c.leader (so that it can still
// be accessed with c.Leader call)
func (c *PartitionClient) disconnect() error {
	if c.conn == nil {
		return nil
	}
	c.conn.Close()
	c.conn = nil
	return nil
}

// Close the connection to the topic partition leader. Nop if no active
// connection. If there is a request in progress blocks until the request
// completes.
func (c *PartitionClient) Close() error { // implement io.Closer
	c.Lock()
	defer c.Unlock()
	c.disconnect()
	return nil
}

// Leader returns the last resolved partition leader, even if connection has
// since been closed (as happens on error).
func (c *PartitionClient) Leader() *Metadata.Broker {
	c.Lock()
	c.Unlock()
	return c.leader
}

func (c *PartitionClient) call(req *api.Request, v interface{}) error {
	c.Lock()
	defer c.Unlock()
	if err := c.connect(); err != nil {
		return err
	}
	// TODO: remove
	if req.ApiKey == api.Produce && c.versions.ApiKeys[api.Produce].MaxVersion == 5 {
		req.ApiVersion = 5 // downgrade to be able to produce to kafka 1.0
	}
	err := call(c.conn, req, v)
	if err != nil {
		c.disconnect()
	}
	return err
}

func (c *PartitionClient) ListOffsets(offset int64) (*ListOffsets.Response, error) {
	req := ListOffsets.NewRequest(c.Topic, c.Partition, offset)
	resp := &ListOffsets.Response{}
	return resp, c.call(req, resp)
}

func (c *PartitionClient) Fetch(args *Fetch.Args) (*Fetch.Response, error) {
	req := Fetch.NewRequest(args)
	resp := &Fetch.Response{}
	return resp, c.call(req, resp)
}

func (c *PartitionClient) Produce(args *Produce.Args, recordSet []byte) (*Produce.Response, error) {
	req := Produce.NewRequest(args, recordSet)
	resp := &Produce.Response{}
	return resp, c.call(req, resp)
}
