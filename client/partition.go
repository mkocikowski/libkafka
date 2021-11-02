package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
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

var (
	ErrPartitionDoesNotExist = errors.New("partition does not exist")
	ErrNoLeaderForPartition  = errors.New("no leader for partition")
)

func GetPartitionLeader(bootstrap string, tlsConfig *tls.Config, topic string, partition int32) (*Metadata.Broker, error) {
	meta, err := CallMetadata(bootstrap, tlsConfig, []string{topic})
	if err != nil {
		return nil, err
	}
	partitions := meta.Partitions(topic)
	if p := partitions[partition]; p == nil {
		return nil, ErrPartitionDoesNotExist
	}
	leaders := meta.Leaders(topic)
	if l := leaders[partition]; l == nil {
		return nil, ErrNoLeaderForPartition
	}
	return leaders[partition], nil
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
	TLS       *tls.Config
	ClientId  string
	Topic     string
	Partition int32
	// ConnMaxIdle corresponds to connections.max.idle.ms broker setting.
	// Kafka broker will close connections that have been idle (no api
	// calls have been made; this is not about tcp keep alives) for this
	// long. Making a PartitionClient call on a closed connection will
	// result in an error (and connection will be re-established on the
	// next call). If you don't want this error, set ConnMaxIdle to >0.
	// This way, if more than ConnMaxIdle passed since the last call,
	// PartitionClient will close the current connection, and open a new
	// one. Default value of 0 means that no check it made.
	ConnMaxIdle  time.Duration
	leader       *Metadata.Broker
	versions     *ApiVersions.Response
	conn         net.Conn
	connOpened   time.Time
	connLastUsed time.Time
}

// if the client has an open connection, check it for libkafka.ConnectionTTL
// and ConnMaxIdle. if these exceeded, close connection, otherwise noop. if
// there is no open connection (or it was just closed because of TTL or
// IdleTimeout) find partition leader, connect to it, and set c.leader
func (c *PartitionClient) connect() (err error) {
	// no mutex here. connect() is called only from call(), and that is
	// where the mutex is acquired for both connect() and disconnect()
	if c.conn != nil {
		switch {
		case libkafka.ConnectionTTL > 0 && time.Since(c.connOpened) > libkafka.ConnectionTTL:
			// connection exceeded TTL
			c.disconnect()
		case c.ConnMaxIdle > 0 && time.Since(c.connLastUsed) > c.ConnMaxIdle:
			// connection exceeded MaxIdle
			c.disconnect()
		default:
			// ConnTTL and ConnMaxIdle do not apply. Leave connection open
			return nil
		}
	}
	c.leader, err = GetPartitionLeader(c.Bootstrap, c.TLS, c.Topic, c.Partition)
	if err != nil {
		return fmt.Errorf("error getting partition leader: %w", err)
	}
	if c.TLS != nil {
		c.conn, err = tls.DialWithDialer(&net.Dialer{Timeout: libkafka.DialTimeout}, "tcp", c.leader.Addr(), c.TLS)
		if err != nil {
			return err
		}
	}
	if c.TLS == nil {
		c.conn, err = net.DialTimeout("tcp", c.leader.Addr(), libkafka.DialTimeout)
		if err != nil {
			return err
		}
	}
	c.connOpened = time.Now().UTC()
	c.connLastUsed = c.connOpened
	// version information is needed only for the kafka 1.0 produce hack
	c.versions, err = apiVersions(c.conn)
	if err != nil {
		return fmt.Errorf("error getting api versions from broker: %w", err)
	}
	if code := c.versions.ErrorCode; code != libkafka.ERR_NONE {
		return fmt.Errorf("error response for api versions call from broker: %w", libkafka.Error{Code: code})
	}
	return nil
}

// close connection to leader, but do not zero c.leader (so that it can still
// be accessed with c.Leader call)
func (c *PartitionClient) disconnect() error {
	// no mutex here. disconnect() is called only from call() and from
	// Close(), and that is where the mutex is acquired.
	if c.conn == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered in PartitionClient.disconnect: %v", r)
		}
	}()
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
	defer c.Unlock()
	return c.leader
}

// Conn returns the connection that the client has to the partition leader. The
// call is safe for concurrent use, but it is no safe to change the connection.
// The purpose of exposing it here is mostly to make it easier to test network
// errors. Be careful with this one. If you need to cleanly close the current
// connection to the leader, call Close(), not Conn().Close().
func (c *PartitionClient) Conn() net.Conn {
	c.Lock()
	defer c.Unlock()
	return c.conn
}

func (c *PartitionClient) call(req *api.Request, v interface{}) error {
	c.Lock()
	defer c.Unlock()
	if err := c.connect(); err != nil {
		return fmt.Errorf("error connecting to partition leader (TLS: %v): %w", c.TLS != nil, err)
	}
	// TODO: remove
	if req.ApiKey == api.Produce && c.versions.ApiKeys[api.Produce].MaxVersion == 5 {
		req.ApiVersion = 5 // downgrade to be able to produce to kafka 1.0
	}
	err := call(c.conn, req, v)
	if err != nil {
		c.disconnect()
		err = fmt.Errorf("error making call to partition leader (TLS: %v): %w", c.TLS != nil, err)
	}
	c.connLastUsed = time.Now().UTC()
	return err
}

func (c *PartitionClient) ListOffsets(timestampMs int64) (*ListOffsets.Response, error) {
	req := ListOffsets.NewRequest(c.Topic, c.Partition, timestampMs)
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
