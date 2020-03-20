package CreateTopics

type Response struct {
	ThrottleTimeMs int32
	Topics         []TopicResponse
}

/*
func (r *Response) Err() error {
	if r.Topics[0].ErrorCode == libkafka.NONE {
		return nil
	}
	return &libkafka.Error{
		Code:    r.Topics[0].ErrorCode,
		Message: r.Topics[0].ErrorMessage,
	}
}
*/

type TopicResponse struct {
	Name         string
	ErrorCode    int16
	ErrorMessage string
}
