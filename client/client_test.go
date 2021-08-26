package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
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

const caCert = `
-----BEGIN CERTIFICATE-----
MIIE+zCCAuOgAwIBAgIUJaP1HoTAKraFQIkBLhshN5vYMmkwDQYJKoZIhvcNAQEL
BQAwDTELMAkGA1UEAwwCY2EwHhcNMjEwODI0MTUzMTE4WhcNMzEwODIyMTUzMTE4
WjANMQswCQYDVQQDDAJjYTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIB
AKbVIfzH+MUnZwE7/QgprKO/+y0Hskwp+s/wbzS64a+Tr6QJSXXkHPvLb7BrPBJO
mEMBl8LbEdY2cx10BQBNURkQVIDZGQeFiHH+fdLhMGk5gOFsUmapZlgpLF1/L6n9
wx27u6EKok4foHwDGx+l6KbQd1LlrydYxRJyD7C5JprA01lP7oalrgVha8jkujg4
k1yfQNusSSQ7+lrwn8eDk4pKYEAQrMJYs1wvmjbzJGUN6tTdQW3lb2DG4KdcLxtO
irifsqtNkEwMnO33JnTkUfF2gSsJ4108BWRTxCcBui0oCiwFvGfGAMMU+ZmrlB9D
p9p2u3rfzhfq7+uT9xA5LutOghR1MORWqpynyjjxPi3wi8WdeZdaQa1/8vnvBos5
64bZ0qVQxu+aG1QAlZmj2pmAiV6KBGXBRv/JoBUPqVIVUdXqTDiPO+lpk5httQJo
Ck0tPwnNqNlpz4U5YNxz60I0I4Gnb4+Ll5P93gPwNVhY0QfZsatIyP2Aikl0uTSB
J9P7h9XlA7/A8wSBnE1Ab6VZiXAVbTjHKSynqDwUQL91QEXXmYKMzWiJ93yhqhqR
uIQ60iD0V/4hMyZ4ZrYGwND8u436vsllf170lojh8Wslig+qy9vra+r7r0AAoa01
HHZcSY4v3uvUdXttb4rw+GOn+zZfEibRE36B8adsdTkBAgMBAAGjUzBRMB0GA1Ud
DgQWBBQagU7UJPX51iQCD/j/QYLvfhSRQjAfBgNVHSMEGDAWgBQagU7UJPX51iQC
D/j/QYLvfhSRQjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQBQ
oAjKa9VXVn2VqT91sbOrUSjrzEB3EOxePZ+IMudhMGI55+SIb7Mfb8xY60B+01bd
U7jNfOylmUkjzq9rBEV1wTY1G7wm8OrIwvrOwSRa35JTRQQyTnv0XJDp/8LIUrjv
+LaQO9/NldhEv3QalUtYlaGuoFoT7uF9b3huO921w5xfWpQ5kwqZcSqfnD3Gk3LY
dE9Kd9ASISgv7+e+2Rssf7UC4tN1Yyap1ypup+CZTwWDo90f1FKWcGmw4NqeZmJd
E13kzv3ZjTRDYnvvqZbo7NNsz4+q7fTX/a+Cs1RhEE1GoI5TjuuzNWMN4DeiYKhG
DcY2Wj5O+wv/deCX1x2cdH4oZcyP5lTbLHHankwRTkX/e3aGZ38baIQlYaSUTgPv
tjUIg185LGjXHDpNPxiIfE7osQ8BRWWtWQdXg8hxqRcVAr6pinTZvls6CcmAzOF4
VHoCHxdheHD7sq+Ah00TBjBMj9lQ6yuvUlze1a1koQKBqUcHK6rEO3DEwX0yyBcJ
Poh/XGHBx2i0s//b21NkmGZhh7haubjw7IfKpfIHh2avkZvET3hN5LqBQzJ3SrJx
2k5UwjpOWZR+4e0Ccc3I0y+cotz5fDVonRI7Sns0wm6vvHOGrkk7PYQ0mVCPfl8T
F68zDvzJO3e6shCo4CGwMWiLhUgvbi/srLjrxCM8OA==
-----END CERTIFICATE-----`

const clientCert = `
-----BEGIN CERTIFICATE-----
MIIE3DCCAsSgAwIBAgIUYiJMrEOuMdT5gc9BDjlJyn1x3o8wDQYJKoZIhvcNAQEL
BQAwDTELMAkGA1UEAwwCY2EwHhcNMjEwODI0MTUzMTE4WhcNMzEwODIyMTUzMTE4
WjARMQ8wDQYDVQQDDAZjbGllbnQwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIK
AoICAQDNswTuUJ0bSvgXxAHdIVPVc2+xCaRwMNGL36iSnkfEpLab1TgLcipMyTZ8
SAN84onUAR9KIi0yCI4uyr0sGXVyTE7y9PCmjT0FmXC2fki2D30jo+mPVjj41omm
Be8qWQO2lLkt8lAuEfILXHPu0pWLYce84UgIwjBXT7E4JnNzWXIyVS/OcmYw6atu
6ykhSKNxDgMl2iJ3zrdr+Arlcs5pDDflTo3rjJ7TowkMKOt7Ba1ZDxX2CfBKrX9a
XxosXVJ3ko38eQaG8lbfsMLeEa5CyNBMwEoa17NHPVx2QEw6uVaolPB4xO9pxTB5
gbn70kmU5/03FwIwYfCN9YDXPoSrSAf3WuHvZX3XPMURIuPdHA4p5bj2r20wBYKg
VjNAlOh9XsSQhyqK7YupFixYthIr9a5Vdbd9vuVYXy3ATEiWUmXB9CbrM4xbyPHU
REn8RcC13t+NZkj8KwozjwxThr3d1v30dv4jBDbciU/xgk54riJ+cftEt4BagE9B
aMUNeCa9KFyUhgBjjGmDokGNjWmCkMaVkfbsH2h8XbGYYI40thA4QMWuUyejpMWL
Ju3bIVMIPIBdUXu8v9JwOOiBMyDnyI8ugbDF4HLJIyQy8fKDaJUgS4rniElFF8tU
IIEKQKHGi5NKzBg/VoR3+gvwElO0y/SIvCT0J6Gxc8Q95Lqo0QIDAQABozAwLjAJ
BgNVHRMEAjAAMAsGA1UdDwQEAwIFoDAUBgNVHREEDTALgglsb2NhbGhvc3QwDQYJ
KoZIhvcNAQELBQADggIBAJA1Q2keEQPtp25TAK7IlSzmjlLBDliU1ZvvxtxouMVG
EVrmD0EvkQcVUj/oGWkRvp4z7u8wSq8/5UdFRh3kDeyqZOMETV5vP+FV66Iw1B+c
AY6yWYSxELBNo8TANlqVIZRTiT7uUGJ6tbJ44t/4U5ncAYHE688yugFHFuTJUt3r
lBsO1+vwShPi2wpho1fI007HorjclRbEx3PnflXlVKJ5gZbwR2eo+XRkILgbXnw6
X6TMUTRijQzjvwXj/PH5iZpXtFQubI8Hz9Cq5e/buPV3iOsHcGwDqsFaPCrWgeFj
uwFxwC2lR2PMo0Md2phFwu1S6OmPDGd2w8kKjkTxb0W0QIlSJsTbfxbucR4xqciu
rOsT6s80BuYNjZF4ok6qmUm0ov1BOT0PCpv+AdYqgu/SadxFpxT944kMFO3442jk
3gJxySDKlfGWbkq5Aoceg17nak6hu2zq5f9ZZYjemCGEGu85MAF427avgj9gBjhm
3dAnmymblYhFup6mqFXSudbFHAz42cSHMZJpiSD80hTKjg1WD7GTQj4ieFP8NVjV
+roWyDNtV7Q1aQPcZE3refUjR8pIZJHDSb/lBE50VUKPw4tOzI18Ux3awhOYmmMl
2K4fpfqnWQUEv7LoSleaq57tW7Zzegw9CHhOUUUAhYr+tHT0YueJTEI3MhRUqi9x
-----END CERTIFICATE-----`

const clientKey = `
-----BEGIN PRIVATE KEY-----
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
hSinI13bploG0ZKF8zHV84OPOf0UwsM=
-----END PRIVATE KEY-----
`

func mTLSConfig() *tls.Config {
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM([]byte(caCert)); !ok {
		panic("!ok")
	}
	cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
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
	if _, err := CallApiVersions("localhost:9092", mTLSConfig()); err == nil {
		t.Fatal("expected error trying to handshake on PLAINTEXT port")
	}
}

func TestIntegrationCallApiVersionsMTLS(t *testing.T) {
	r, err := CallApiVersions("localhost:9093", mTLSConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", r)
	if _, err := CallApiVersions("localhost:9093", nil); err == nil {
		t.Fatal("expected error trying PLAINTEXT connection on TLS port")
	}
}

// this test will pass or fail depending on the ssl.client.auth broker setting.
// for mTLS to be "on" the setting must be 'ssl.client.auth=required' and that
// is what i have it set for the libkafka integration tests.
func TestIntegrationCallApiVersionsTLSNoClientCert(t *testing.T) {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(caCert))
	_, err := CallApiVersions("localhost:9093", &tls.Config{RootCAs: pool})
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
