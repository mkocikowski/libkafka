package varint

import (
	"math"
	"testing"
)

func TestZigZag64(t *testing.T) {
	tests := []int64{0, 1, -1, math.MaxInt32, math.MinInt32, math.MaxInt64, math.MinInt64}
	for _, tt := range tests {
		b := EncodeZigZag64(tt)
		i, _ := DecodeZigZag64(b)
		if i != tt {
			t.Fatal(tt, i)
		}
		//t.Log(tt, b, i)
	}
}
