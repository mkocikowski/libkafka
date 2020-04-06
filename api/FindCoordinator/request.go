package FindCoordinator

import (
	"github.com/mkocikowski/libkafka/api"
)

const (
	CoordinatorGroup = iota
	CoordinatorTransaction
)

func NewRequest(groupId string) *api.Request {
	return &api.Request{
		ApiKey:     api.FindCoordinator,
		ApiVersion: 1,
		Body: Request{
			Key:     groupId,
			KeyType: CoordinatorGroup,
		},
	}
}

type Request struct {
	Key     string // groupId
	KeyType int8
}
