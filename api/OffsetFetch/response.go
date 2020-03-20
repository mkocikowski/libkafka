package OffsetFetch

type Response struct {
	ThrottleTimeMs int32
	Topics         []TopicResponse
	ErrorCode      int16
}

type TopicResponse struct {
	Name       string
	Partitions []PartitionResponse
}

type PartitionResponse struct {
	PartitionIndex int32
	CommitedOffset int64
	Metadata       string
	ErrorCode      int16
}
