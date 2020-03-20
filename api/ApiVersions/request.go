package ApiVersions

import (
	"github.com/mkocikowski/libkafka/api"
)

func NewRequest() *api.Request {
	return &api.Request{
		ApiKey:     api.ApiVersions,
		ApiVersion: 0,
	}
}
