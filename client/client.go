// Package client has code for making api calls to brokers. It implements the
// PartitionClient which maintains a connection to a single partition leader
// (producers and consumers are built on top of that) and the GroupClient which
// maintains a connection to the group manager (for group membership and for
// offset management).  Clients are synchronous and all code executes in the
// calling goroutine.
package client

import (
	"bufio"
	"crypto/tls"
	"errors"
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

	errNotAnSRV = fmt.Errorf("not an SRV")
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
		return nil, fmt.Errorf("%w: %s", errNotAnSRV, err)
	}
	var addrs []string
	for _, srv := range srvs {
		hosts, err := net.LookupHost(srv.Target)
		// skip hosts that can't be resolved
		if err != nil || len(hosts) == 0 {
			continue
		}
		host := net.JoinHostPort(hosts[0], strconv.Itoa(int(srv.Port)))
		addrs = append(addrs, host)
	}
	if len(addrs) < 1 {
		return nil, fmt.Errorf("failed to resolve SRV record %q: no hosts found", name)
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

// randomBroker tries to resolve name through a call to lookupSrv. If successful
// it returns a random host:port from the list. lookupSrv in its turn invokes
// net.LookupSRV(), unsuccessful result is considered as not an SRV record, so
// you can pass "localhost:9092" for example. It returns error, if resolved SRV
// record has no hosts.
func randomBroker(name string) (string, error) {
	addrs, err := lookupSrv(name)
	if err != nil {
		if errors.Is(err, errNotAnSRV) {
			return name, nil
		}
		return "", err
	}

	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return addrs[0], nil
}

func connectToRandomBroker(bootstrap string, tlsConfig *tls.Config) (net.Conn, error) {
	host, err := randomBroker(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("failed to get random broker: %w", err)
	}
	if tlsConfig != nil {
		return tls.DialWithDialer(&net.Dialer{Timeout: libkafka.DialTimeout}, "tcp", host, tlsConfig)
	}
	return net.DialTimeout("tcp", host, libkafka.DialTimeout)
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
func connectToRandomBrokerAndCall(bootstrap string, tlsConfig *tls.Config, req *api.Request, v interface{}) (err error) {
	defer func() {
		if err != nil {
			forgetSrv(bootstrap)
		}
	}()
	var conn net.Conn
	if conn, err = connectToRandomBroker(bootstrap, tlsConfig); err != nil {
		return fmt.Errorf("error connecting to random broker (TLS: %v): %w", tlsConfig != nil, err)
	}
	defer conn.Close()
	if err := call(conn, req, v); err != nil {
		return fmt.Errorf("error making call to random broker (TLS: %v): %w", tlsConfig != nil, err)
	}
	return nil
}

func CallApiVersions(bootstrap string, tlsConfig *tls.Config) (*ApiVersions.Response, error) {
	req := ApiVersions.NewRequest()
	resp := &ApiVersions.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, tlsConfig, req, resp)
}

func apiVersions(conn net.Conn) (*ApiVersions.Response, error) {
	req := ApiVersions.NewRequest()
	resp := &ApiVersions.Response{}
	return resp, call(conn, req, resp)
}

func CallMetadata(bootstrap string, tlsConfig *tls.Config, topics []string) (*Metadata.Response, error) {
	req := Metadata.NewRequest(topics)
	resp := &Metadata.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, tlsConfig, req, resp)
}

func CallCreateTopic(bootstrap string, tlsConfig *tls.Config, topic string, numPartitions int32, replicationFactor int16) (*CreateTopics.Response, error) {
	req := CreateTopics.NewRequest(topic, numPartitions, replicationFactor, []CreateTopics.Config{})
	resp := &CreateTopics.Response{}
	return resp, connectToRandomBrokerAndCall(bootstrap, tlsConfig, req, resp)
}
