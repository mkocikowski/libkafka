package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/mkocikowski/libkafka"
	"github.com/mkocikowski/libkafka/api/CreateTopics"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func mTLSConfig() *tls.Config {
	caCert, err := ioutil.ReadFile("../test/mtls/ca-cert.pem")
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
		panic("!ok")
	}
	cert, err := tls.LoadX509KeyPair("../test/mtls/client-cert.pem", "../test/mtls/client-key.pem")
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}
}

func TestIntegrationCallApiVersions(t *testing.T) {
	r, err := CallApiVersions("localhost:9092", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
}

func TestIntegrationCallApiVersionsMTLS(t *testing.T) {
	conf := mTLSConfig()
	// make good call with CAs and client certs in order
	r, err := CallApiVersions("localhost:9093", mTLSConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
	// try to make TLS call on PLAINTEXT port
	if _, err = CallApiVersions("localhost:9092", mTLSConfig()); err == nil {
		t.Fatal("expected error trying to handshake on PLAINTEXT port")
	}
	t.Log(err)
	// try to make PLAINTEXT call on TLS port
	if _, err = CallApiVersions("localhost:9093", nil); err == nil {
		t.Fatal("expected error trying PLAINTEXT connection on TLS port")
	}
	t.Log(err)
	// If RootCAs is nil, TLS uses the host's root CA set. Expect cert error
	conf.RootCAs = nil
	if _, err = CallApiVersions("localhost:9093", conf); err == nil {
		t.Fatal("expected 'x509: certificate signed by unknown authority' error")
	}
	t.Log(err)
	// conf.InsecureSkipVerify=true will skip checking broker cert chain. Expect success
	conf.InsecureSkipVerify = true
	if _, err = CallApiVersions("localhost:9093", conf); err != nil {
		t.Fatal(err)
	}
}

// this test will pass or fail depending on the ssl.client.auth broker setting.
// for mTLS to be "on" the setting must be 'ssl.client.auth=required' and that
// is what i have it set for the libkafka integration tests.
func TestIntegrationCallApiVersionsTLSNoClientCert(t *testing.T) {
	caCert, err := ioutil.ReadFile("../test/mtls/ca-cert.pem")
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	_, err = CallApiVersions("localhost:9093", &tls.Config{RootCAs: pool})
	if err == nil {
		t.Fatal("expected 'tls: unexpected message' error if 'ssl.client.auth=required' in broker config")
	}
	t.Log(err)
}

func TestIntegrationCallApiVersionsBadHost(t *testing.T) {
	_, err := CallApiVersions("foo", nil)
	if err == nil {
		t.Fatal("expected bad host error")
	}
	t.Log(err)
}

func TestIntegrationCallCreateTopic(t *testing.T) {
	brokers := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	var r *CreateTopics.Response
	r, _ = CallCreateTopic(brokers, nil, topic, 1, 2)
	if r.Topics[0].ErrorCode != libkafka.ERR_INVALID_REPLICATION_FACTOR {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	r, _ = CallCreateTopic(brokers, nil, topic, 1, 1)
	if r.Topics[0].ErrorCode != libkafka.ERR_NONE {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	r, _ = CallCreateTopic(brokers, nil, topic, 1, 1)
	if r.Topics[0].ErrorCode != libkafka.ERR_TOPIC_ALREADY_EXISTS {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	if _, err := CallCreateTopic("foo:9092", nil, topic, 1, 1); err == nil {
		t.Fatal("expected error calling foo broker")
	}
	// TLS
	r, _ = CallCreateTopic("localhost:9093", mTLSConfig(), topic, 1, 1)
	if r.Topics[0].ErrorCode != libkafka.ERR_TOPIC_ALREADY_EXISTS {
		t.Fatal(&libkafka.Error{Code: r.Topics[0].ErrorCode})
	}
	if _, err := CallCreateTopic("localhost:9093", nil, topic, 1, 1); err == nil {
		t.Fatal("expected error calling TLS port without tls config")
	}
}

func TestIntegrationCallCreateTopicRequestTimeout(t *testing.T) {
	d := libkafka.RequestTimeout
	defer func() {
		libkafka.RequestTimeout = d
	}()
	libkafka.RequestTimeout = time.Nanosecond
	brokers := "localhost:9092"
	topic := fmt.Sprintf("test-%x", rand.Uint32())
	_, err := CallCreateTopic(brokers, nil, topic, 1, 2)
	for {
		err = errors.Unwrap(err)
		if err == nil {
			break
		}
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return // success
		}
	}
	t.Fatalf("expected timeout got %v", err)
}

func TestUnitConnectToRandomBrokerAndCallErrorForgetSRV(t *testing.T) {
	srvLookupCache["foo"] = []string{"bar:1"}
	err := connectToRandomBrokerAndCall("foo", nil, nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := srvLookupCache["foo"]; ok {
		t.Fatal("expected key to be deleted because of call error")
	}
}
