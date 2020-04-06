package SyncGroup

// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(group, member string, generation int32, assignments []Assignment) *api.Request {
	return &api.Request{
		ApiKey:     api.SyncGroup,
		ApiVersion: 1,
		Body: Request{
			GroupId:      group,
			GenerationId: generation,
			MemberId:     member,
			Assignments:  assignments,
		},
	}
}

type Request struct {
	GroupId      string
	GenerationId int32
	MemberId     string
	Assignments  []Assignment
}

type Assignment struct {
	MemberId   string
	Assignment []byte
}
