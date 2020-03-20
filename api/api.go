// Package api defines Kafka protocol requests and responses.
package api

const (
	Produce                 int16 = 0 // v 5
	Fetch                         = 1 // v 6
	ListOffsets                   = 2 // v 2
	Metadata                      = 3 // v 5
	LeaderAndIsr                  = 4
	StopReplica                   = 5
	UpdateMetadata                = 6
	ControlledShutdown            = 7
	OffsetCommit                  = 8  // v 2 implemented, v 3 max for 1.0
	OffsetFetch                   = 9  // v 3
	FindCoordinator               = 10 // v 1
	JoinGroup                     = 11 // v 2 ; v 4 in kafka 2.2 allows to provide member_id for static assignments: https://github.com/apache/kafka/pull/6058/files
	Heartbeat                     = 12
	LeaveGroup                    = 13
	SyncGroup                     = 14
	DescribeGroups                = 15
	ListGroups                    = 16
	SaslHandshake                 = 17
	ApiVersions                   = 18 // v 0 TODO: 1
	CreateTopics                  = 19 // v 2
	DeleteTopics                  = 20
	DeleteRecords                 = 21
	InitProducerId                = 22
	OffsetForLeaderEpoch          = 23
	AddPartitionsToTxn            = 24
	AddOffsetsToTxn               = 25
	EndTxn                        = 26
	WriteTxnMarkers               = 27
	TxnOffsetCommit               = 28
	DescribeAcls                  = 29
	CreateAcls                    = 30
	DeleteAcls                    = 31
	DescribeConfigs               = 32
	AlterConfigs                  = 33
	AlterReplicaLogDirs           = 34
	DescribeLogDirs               = 35
	SaslAuthenticate              = 36
	CreatePartitions              = 37
	CreateDelegationToken         = 38
	RenewDelegationToken          = 39
	ExpireDelegationToken         = 40
	DescribeDelegationToken       = 41
	DeleteGroups                  = 42
	ElectPreferredLeaders         = 43
)

var Keys = map[int]string{
	0:  "Produce",
	1:  "Fetch",
	2:  "ListOffsets",
	3:  "Metadata",
	4:  "LeaderAndIsr",
	5:  "StopReplica",
	6:  "UpdateMetadata",
	7:  "ControlledShutdown",
	8:  "OffsetCommit",
	9:  "OffsetFetch",
	10: "FindCoordinator",
	11: "JoinGroup",
	12: "Heartbeat",
	13: "LeaveGroup",
	14: "SyncGroup",
	15: "DescribeGroups",
	16: "ListGroups",
	17: "SaslHandshake",
	18: "ApiVersions",
	19: "CreateTopics",
	20: "DeleteTopics",
	21: "DeleteRecords",
	22: "InitProducerId",
	23: "OffsetForLeaderEpoch",
	24: "AddPartitionsToTxn",
	25: "AddOffsetsToTxn",
	26: "EndTxn",
	27: "WriteTxnMarkers",
	28: "TxnOffsetCommit",
	29: "DescribeAcls",
	30: "CreateAcls",
	31: "DeleteAcls",
	32: "DescribeConfigs",
	33: "AlterConfigs",
	34: "AlterReplicaLogDirs",
	35: "DescribeLogDirs",
	36: "SaslAuthenticate",
	37: "CreatePartitions",
	38: "CreateDelegationToken",
	39: "RenewDelegationToken",
	40: "ExpireDelegationToken",
	41: "DescribeDelegationToken",
	42: "DeleteGroups",
	43: "ElectPreferredLeaders",
}
