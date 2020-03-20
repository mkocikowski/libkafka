package FindCoordinator

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/mkocikowski/libkafka/wire"
)

//const key = `AAEAJ2NsaWNraG91c2UtcmVxdWVzdHMtYm90LWRldGVjdGlvbi1wcm9kMgAWcmVxdWVzdHNfYm90X2RldGVjdGlvbgAAAAE=`

func TestUnitGroup(t *testing.T) {
	//const fixture = `AAEAIHdvcmtlcnMtcnVudGltZS1pbnNlcnRlci1zdGFnaW5nAA93b3JrZXJzX3J1bnRpbWUAAABK`
	const fixture = `AAEAIHdvcmtlcnMtcnVudGltZS1pbnNlcnRlci1zdGFnaW5nAA93b3JrZXJzX3J1bnRpbWUAAABK`
	b, _ := base64.StdEncoding.DecodeString(fixture)
	r := &Group{}
	wire.Read(bytes.NewReader(b), reflect.ValueOf(r))
	t.Logf("%+v", r)
}

const val = `AAEAAAATknd9pgAAAAABcCByu6EAAAFwJZkXoQ==`

func TestUnitVal(t *testing.T) {
	//const fixture = `AAEAAAAUSj2EhQAAAAABcD//s0cAAAFwRSYPRw==`
	const fixture = `AAEAAAATkWaDEAAAAAABcCBNVWoAAAFwJXOxag==`
	b, _ := base64.StdEncoding.DecodeString(fixture)
	r := &Val{}
	wire.Read(bytes.NewReader(b), reflect.ValueOf(r))
	t.Logf("%+v", r)
}
