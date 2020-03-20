package OffsetCommit

type Response struct {
	Topics []TopicResponse
}

type TopicResponse struct {
	Name       string
	Partitions []PartitionResponse
}

type PartitionResponse struct {
	PartitionIndex int32
	ErrorCode      int16
}
