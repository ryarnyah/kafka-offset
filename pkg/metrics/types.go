package metrics

// KafkaOffsetMetric metric for topic/partition with oldest and newest offset
type KafkaOffsetMetric struct {
	Topic        string
	Partition    int32
	OldestOffset int64
	NewestOffset int64
}

// KafkaConsumerGroupOffsetMetric metric for consumer group
type KafkaConsumerGroupOffsetMetric struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Lag       int64
}
