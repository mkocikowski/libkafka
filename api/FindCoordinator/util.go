package FindCoordinator

// https://github.com/apache/kafka/blob/2.4/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1301
type Group struct {
	Version   int16
	Group     string
	Topic     string
	Partition int32
}

// https://github.com/apache/kafka/blob/2.4/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1330
type Val struct {
	Version   int16
	Offset    int64
	Metadata  string
	Timestamp int64
}
