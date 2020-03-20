package Metadata

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest(topics []string) *api.Request {
	return &api.Request{
		ApiKey:     api.Metadata,
		ApiVersion: 5,
		Body: Request{
			Topics:                 topics,
			AllowAutoTopicCreation: false,
		},
	}
}

type Request struct {
	Topics                 []string
	AllowAutoTopicCreation bool
}
