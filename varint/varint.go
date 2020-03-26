// Package varint implements varint and ZigZag encoding and decoding.
package varint

// https://github.com/gogo/protobuf/blob/master/proto/encode.go#L153
func EncodeZigZag64(x int64) []byte {
	// use signed number to get arithmetic right shift.
	//return EncodeVarint(uint64((x << 1) ^ uint64((int64(x) >> 63))))
	return EncodeVarint(uint64(x<<1 ^ (x >> 63)))
}

// https://github.com/gogo/protobuf/blob/master/proto/decode.go#L242
func DecodeZigZag64(buf []byte) (int64, int) {
	x, n := DecodeVarint(buf)
	x = (x >> 1) ^ uint64((int64(x&1)<<63)>>63)
	return int64(x), n
}

const maxVarintBytes = 10 // maximum length of a varint

// https://github.com/golang/protobuf/blob/master/proto/encode.go#L72
func EncodeVarint(x uint64) []byte {
	var buf [maxVarintBytes]byte
	var n int
	for n = 0; x > 127; n++ {
		buf[n] = 0x80 | uint8(x&0x7F)
		x >>= 7
	}
	buf[n] = uint8(x)
	n++
	return buf[0:n]
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
