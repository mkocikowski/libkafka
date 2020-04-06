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
	"io"
	"math/rand"
	"net"
	"strconv"

	"github.com/mkocikowski/libkafka/api"
	"github.com/mkocikowski/libkafka/api/ApiVersions"
	"github.com/mkocikowski/libkafka/api/CreateTopics"
	"github.com/mkocikowski/libkafka/api/Metadata"
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

func GetPartitionLeaders(bootstrap, topic string) (map[int32]*Metadata.Broker, error) {
	v, err := GetMetadata(bootstrap, []string{topic})
	if err != nil {
		return nil, err
	}
	return v.Leaders(topic), nil
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
