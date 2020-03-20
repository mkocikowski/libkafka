package wire

import (
	"bytes"
	"reflect"
	"testing"
)

type Outer struct {
	Int16       int16
	Int16Array  []int16
	Struct      Inner
	StructArray []Inner
}

type Inner struct {
	Int16 int16
}

func TestWriteRead(t *testing.T) {
	m := &Outer{
		Int16:       1,
		Int16Array:  []int16{2, 3},
		Struct:      Inner{4},
		StructArray: []Inner{Inner{5}, Inner{6}},
	}
	t.Logf("%+v", m)
	buf := new(bytes.Buffer)
	if err := Write(buf, reflect.ValueOf(m)); err != nil {
		t.Fatal(err)
	}
	b := buf.Bytes()
	t.Log(b)
	n := &Outer{}
	if err := Read(bytes.NewReader(b), reflect.ValueOf(n)); err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", n)
}
