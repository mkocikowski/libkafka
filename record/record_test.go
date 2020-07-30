package record

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"
)

func TestUnitMarshal(t *testing.T) {
	tests := []struct {
		r     *Record
		key   []byte
		value []byte
	}{
		{New([]byte(""), []byte("m1")), nil, []byte("m1")},
		{New([]byte("foo"), []byte("m1")), []byte("foo"), []byte("m1")},
		{New(nil, nil), nil, nil},
		{New(nil, make([]byte, 10)), nil, make([]byte, 10)},
		{New(nil, make([]byte, 1e5)), nil, make([]byte, 1e5)},
	}

	x1 := make([]byte, 100)
	x2 := make([]byte, 100)
	buf := new(bytes.Buffer)

	for _, test := range tests {
		b := test.r.Marshal()
		// make sure Marshal2 and Marshal3 are equivalent
		b2 := test.r.Marshal2(make([]byte, 0, 1<<10))
		if !bytes.Equal(b, b2) {
			t.Fatal(b, b2)
		}
		b3 := test.r.Marshal3()
		if !bytes.Equal(b, b3) {
			t.Fatal(b, b3)
		}
		buf.Reset()
		test.r.Marshal4(x1, x2, buf)
		b4 := buf.Bytes()
		if !bytes.Equal(b, b4) {
			t.Fatal(b, b4)
		}
		//
		t.Logf("%v %s", b, base64.StdEncoding.EncodeToString(b))
		r, _ := Unmarshal(b)
		if !bytes.Equal(r.Key, test.key) {
			t.Fatal(r.Key, test.key)
		}
		if !bytes.Equal(r.Value, test.value) {
			t.Fatal(r.Value, test.value)
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

// this benchmark is not representative of workloads where record values are
// >100B. it seems like value len has the biggest impact (because of slice
// growing). see additional benchmarks below.
func BenchmarkRecord_MarshalSmall(b *testing.B) {
	const messagesN = 1e3
	msgs := make([]*Record, messagesN)
	for i := 0; i < messagesN; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("value_%d", i)
		r := New([]byte(key), []byte(val))
		r.Attributes = int8(i)
		r.TimestampDelta = rand.Int63()
		r.OffsetDelta = rand.Int63()
		msgs[i] = r
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b := msgs[i%messagesN].Marshal()
		b = b[:]
	}
}

func BenchmarkRecord_Marshal(b *testing.B) {
	for i := 7; i < 17; i++ {
		size := 1 << i
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			r := New(make([]byte, 27), make([]byte, size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Marshal()
			}
		})
	}
}

func BenchmarkRecord_Marshal2(b *testing.B) {
	for i := 7; i < 17; i++ {
		size := 1 << i
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			r := New(make([]byte, 27), make([]byte, size))
			b.ReportAllocs()
			b.ResetTimer()
			tmp := make([]byte, 0, 256<<10)
			for i := 0; i < b.N; i++ {
				r.Marshal2(tmp)
				tmp = tmp[0:0]
			}
		})
	}
}

func BenchmarkRecord_Marshal3(b *testing.B) {
	for i := 7; i < 17; i++ {
		size := 1 << i
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			r := New(make([]byte, 27), make([]byte, size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Marshal3()
			}
		})
	}
}

func BenchmarkRecord_Marshal4(b *testing.B) {
	for i := 7; i < 17; i++ {
		size := 1 << i
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			r := New(make([]byte, 27), make([]byte, size))
			x1 := make([]byte, 4<<10)
			x2 := make([]byte, 4<<10)
			buf := bytes.NewBuffer(make([]byte, 1<<20))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.Marshal4(x1, x2, buf)
				buf.Reset()
			}
		})
	}
}
