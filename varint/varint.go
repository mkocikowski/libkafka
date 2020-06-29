// Package varint implements varint and ZigZag encoding and decoding.
package varint

// PutZigZag64 encodes an int64 into dst using buf as buffer.
func PutZigZag64(dst, buf []byte, x int64) []byte {
	// use signed number to get arithmetic right shift.
	n := PutVarint(buf, uint64(x<<1^(x>>63)))
	return append(dst, buf[:n]...)
}

// https://github.com/gogo/protobuf/blob/master/proto/decode.go#L242
func DecodeZigZag64(buf []byte) (int64, int) {
	x, n := DecodeVarint(buf)
	x = (x >> 1) ^ uint64((int64(x&1)<<63)>>63)
	return int64(x), n
}

// PutVarint encodes an int64 into buf and returns the number of bytes written.
// If the buffer is too small, PutVarint will panic.
// Based on https://github.com/golang/protobuf/blob/master/proto/encode.go#L72
func PutVarint(buf []byte, x uint64) int {
	n := 0
	for n = 0; x > 127; n++ {
		buf[n] = 0x80 | uint8(x&0x7F)
		x >>= 7
	}
	buf[n] = uint8(x)
	return n + 1
}

// https://github.com/golang/protobuf/blob/master/proto/decode.go#L57
func DecodeVarint(buf []byte) (x uint64, n int) {
	for shift := uint(0); shift < 64; shift += 7 {
		if n >= len(buf) {
			return 0, 0
		}
		b := uint64(buf[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return x, n
		}
	}
	// The number is too large to represent in a 64-bit value.
	return 0, 0
}
