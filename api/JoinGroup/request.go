package JoinGroup

// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(group, member, protocol string, protocols []Protocol) *api.Request {
	return &api.Request{
		ApiKey:     api.JoinGroup,
		ApiVersion: 2,
		Body: Request{
			GroupId:            group,
			SessionTimeoutMs:   10000, // if no heartbeat this long then rebalance
			RebalanceTimeoutMs: 5000,  // wait this long for members to join
			MemberId:           member,
			ProtocolType:       protocol,
			Protocols:          protocols,
		},
	}
}

type Request struct {
	GroupId            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberId           string
	ProtocolType       string
	Protocols          []Protocol
}

type Protocol struct {
	Name     string
	Metadata []byte
}
