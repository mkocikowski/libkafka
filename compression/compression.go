package compression

// https://kafka.apache.org/documentation/#recordbatch
const (
	None = iota
	Gzip
	Snappy
	Lz4
	Zstd

	/*
		TimestampCreate    = 0b0000
		TimestampLogAppend = 0b1000
	*/
)

// Nop implements the batch.Compressor and batch.Decompressor. Use it to
// marshal and unmarshal uncompressed record batches.
type Nop struct{}

func (*Nop) Compress(b []byte) ([]byte, error)   { return b, nil }
func (*Nop) Decompress(b []byte) ([]byte, error) { return b, nil }
func (*Nop) Type() int16                         { return None }
