package Heartbeat

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(group, member string, generation int32) *api.Request {
	return &api.Request{
		ApiKey:     api.Heartbeat,
		ApiVersion: 1,
		Body: Request{
			GroupId:      group,
			GenerationId: generation,
			MemberId:     member,
		},
	}
}

type Request struct {
	GroupId      string
	GenerationId int32
	MemberId     string
}
