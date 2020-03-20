package ListOffsets

type Response struct {
	ThrottleTimeMs int32
	Responses      []TopicResponse
}

type TopicResponse struct {
	Topic      string
	Partitions []PartitionResponse
}

type PartitionResponse struct {
	Partition int32
	ErrorCode int16
	Timestamp int64
	Offset    int64
}

func (r *Response) Offset(topic string, partition int32) int64 {
	for _, t := range r.Responses {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.Partition != partition {
				continue
			}
			return p.Offset
		}
	}
	return -1
}
