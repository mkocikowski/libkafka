package record

import (
	"encoding/base64"
	"testing"
)

func TestUnitMarshal(t *testing.T) {
	tests := []struct {
		r   *Record
		key string
		val string
	}{
		{New(nil, []byte("m1")), "", "m1"},
		{New([]byte("foo"), []byte("m1")), "foo", "m1"},
		{New(nil, nil), "", ""},
	}

	for _, test := range tests {
		b := test.r.Marshal()
		t.Logf("%v %s", b, base64.StdEncoding.EncodeToString(b))
		r, _ := Unmarshal(b)
		if string(r.Key) != test.key {
			t.Fatal(string(r.Key))
		}
		if string(r.Value) != test.val {
			t.Fatal(string(r.Value))
		}
	}
}

const recordBodyFixture = `EAAABAEEbTMA`

func TestUnitUnmarshal(t *testing.T) {
	b, _ := base64.StdEncoding.DecodeString(recordBodyFixture)
	t.Log(len(b))
	r, _ := Unmarshal(b)
	t.Logf("%+v", r)
	if string(r.Value) != "m3" {
		t.Fatal(string(r.Value))
	}
}
