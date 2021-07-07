// Package client has code for making api calls to brokers. It implements the
// PartitionClient which maintains a connection to a single partition leader
// (producers and consumers are built on top of that) and the GroupClient which
// maintains a connection to the group manager (for group membership and for
// offset management).  Clients are synchronous and all code executes in the
// calling goroutine.
package client

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/ApiVersions"
	"github.com/mkocikowski/libkafka/api/CreateTopics"
	"github.com/mkocikowski/libkafka/api/Metadata"
)

var (
	srvLookupMutex sync.Mutex
	srvLookupCache = make(map[string][]string) // TODO: ttl
)

func lookupSrv(name string) ([]string, error) {
	srvLookupMutex.Lock()
	defer srvLookupMutex.Unlock()
	if addrs, ok := srvLookupCache[name]; ok {
		addrsCopy := make([]string, len(addrs))
		copy(addrsCopy, addrs) // making copy because it will be mutated
		return addrsCopy, nil
	}
	_, srvs, err := net.LookupSRV("", "", name)
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, srv := range srvs {
		hosts, err := net.LookupHost(srv.Target)
		if err != nil {
			return nil, err
		}
		if len(hosts) == 0 {
			return nil, fmt.Errorf("unknown error looking up host %v for srv record %v", srv.Target, name)
		}
		host := net.JoinHostPort(hosts[0], strconv.Itoa(int(srv.Port)))
		addrs = append(addrs, host)
	}
	srvLookupCache[name] = addrs
	addrsCopy := make([]string, len(addrs))
	copy(addrsCopy, addrs) // making copy because it will be mutated
	return addrsCopy, nil
}

func forgetSrv(name string) {
	srvLookupMutex.Lock()
	delete(srvLookupCache, name)
	srvLookupMutex.Unlock()
}

// randomBroker tries to resolve name through a call to lookupSrv. If
// successful it returns a random host:port from the list. If lookupSrv fails
// it returns name unmodified (so you can pass "localhost:9092" for example).
func randomBroker(name string) string {
	addrs, err := lookupSrv(name)
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

func connectToRandomBroker(bootstrap string) (net.Conn, error) {
	return net.DialTimeout("tcp", randomBroker(bootstrap), libkafka.DialTimeout)
}

func call(conn net.Conn, req *api.Request, v interface{}) error {
	if libkafka.RequestTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(libkafka.RequestTimeout)); err != nil {
			return fmt.Errorf("failed to set connection deadline: %w", err)
		}
	}
	out := bufio.NewWriter(conn)
	if _, err := out.Write(req.Bytes()); err != nil {
		return fmt.Errorf("error sending %T request: %w", req.Body, err)
	}
	if err := out.Flush(); err != nil {
		return fmt.Errorf("error finalizing %T request: %w", req.Body, err)
	}
	resp, err := api.Read(bufio.NewReader(conn))
	if err != nil {
		return fmt.Errorf("error reading %T response: %w", req.Body, err)
	}
	if err := resp.Unmarshal(v); err != nil {
		return fmt.Errorf("error unmarshaling %T response: %w", req.Body, err)
	}
	return nil
}

// this is used for calls that do not need to talk to a specific broker (such
// as the Metadata call). these are the "bootstrap" calls. if bootstrap is an
// srv record, that record gets resolved and that resolved value is cached.
// that cached value is cleared on call error (for example: srv record pointed
// to a host that used to be a kafka broker but no longer is).
func connectToRandomBrokerAndCall(bootstrap string, req *api.Request, v interface{}) (err error) {
	defer func() {
		if err != nil {
			forgetSrv(bootstrap)
		}
	}()
	var conn net.Conn
	if conn, err = connectToRandomBroker(bootstrap); err != nil {
		return err
	}
	defer conn.Close()
	return call(conn, req, v)
}

func CallApiVersions(bootstrap string) (*ApiVersions.Response, error) {
	req := ApiVersions.NewRequest()
	resp := &ApiVersions.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, req, resp)
}

func apiVersions(conn net.Conn) (*ApiVersions.Response, error) {
	req := ApiVersions.NewRequest()
	resp := &ApiVersions.Response{}
	return resp, call(conn, req, resp)
}

func CallMetadata(bootstrap string, topics []string) (*Metadata.Response, error) {
	req := Metadata.NewRequest(topics)
	resp := &Metadata.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, req, resp)
}

func CallCreateTopic(bootstrap, topic string, numPartitions int32, replicationFactor int16) (*CreateTopics.Response, error) {
	req := CreateTopics.NewRequest(topic, numPartitions, replicationFactor, []CreateTopics.Config{})
	resp := &CreateTopics.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, req, resp)
}
