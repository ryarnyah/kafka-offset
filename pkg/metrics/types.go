package metrics

import "time"

// KafkaOffsetMetric metric for topic/partition with oldest and newest offset
type KafkaOffsetMetric struct {
	Name         string
	Timestamp    time.Time
	Topic        string
	Partition    int32
	OldestOffset int64
	NewestOffset int64
}

// KafkaConsumerGroupOffsetMetric metric for consumer group
type KafkaConsumerGroupOffsetMetric struct {
	Name      string
	Timestamp time.Time
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Lag       int64
}

// KafkaTopicRateMetric rate topic writes per seconds
type KafkaTopicRateMetric struct {
	Name      string
	Timestamp time.Time
	Topic     string
	Rate1     float64
	Rate5     float64
	Rate15    float64
	RateMean  float64
	Count     int64
}

// KafkaConsumerGroupRateMetric rate consumer group read/commit per seconds
type KafkaConsumerGroupRateMetric struct {
	Name      string
	Timestamp time.Time
	Group     string
	Topic     string
	Rate1     float64
	Rate5     float64
	Rate15    float64
	RateMean  float64
	Count     int64
}
