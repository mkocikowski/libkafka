package Metadata

import (
	"net"
	"strconv"
)

type Response struct {
	ThrottleTimeMs int32
	Brokers        []Broker
	ClusterId      string
	ControllerId   int32
	TopicMetadata  []TopicMetadata
}

type Broker struct {
	NodeId int32
	Host   string
	Port   int32
	Rack   string
}

func (b *Broker) Addr() string {
	return net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port)))
}

type TopicMetadata struct {
	ErrorCode         int16
	Topic             string
	IsInternal        bool
	PartitionMetadata []PartitionMetadata
}

type PartitionMetadata struct {
	ErrorCode       int16
	Partition       int32
	Leader          int32
	Replicas        []int32
	Isr             []int32
	OfflineReplicas []int32
}

func (r *Response) Broker(id int32) *Broker {
	for _, b := range r.Brokers {
		if b.NodeId == id {
			return &b
		}
	}
	return nil
}

func (r *Response) Leaders(topic string) map[int32]*Broker {
	leaders := make(map[int32]*Broker)
	for _, t := range r.TopicMetadata {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.PartitionMetadata {
			broker := r.Broker(p.Leader)
			if broker != nil {
				leaders[p.Partition] = broker
			}
		}
	}
	return leaders
}
