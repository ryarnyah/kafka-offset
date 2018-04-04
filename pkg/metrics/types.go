package metrics

import "time"

// KafkaOffsetMetric metric for topic/partition with oldest and newest offset
type KafkaOffsetMetric struct {
	Timestamp    time.Time `json:"timestamp,omitempty"`
	Topic        string    `json:"topic,omitempty"`
	Partition    int32     `json:"partition,omitempty"`
	OldestOffset int64     `json:"oldest_offset,omitempty"`
	NewestOffset int64     `json:"newest_offset,omitempty"`
}

// KafkaConsumerGroupOffsetMetric metric for consumer group
type KafkaConsumerGroupOffsetMetric struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Group     string    `json:"group,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Partition int32     `json:"partition,omitempty"`
	Offset    int64     `json:"offset,omitempty"`
	Lag       int64     `json:"lag,omitempty"`
}

// KafkaTopicRateMetric rate topic writes per seconds
type KafkaTopicRateMetric struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Rate1     float64   `json:"rate1,omitempty"`
	Rate5     float64   `json:"rate5,omitempty"`
	Rate15    float64   `json:"rate15,omitempty"`
	RateMean  float64   `json:"rate_mean,omitempty"`
	Count     int64     `json:"count,omitempty"`
}

// KafkaConsumerGroupRateMetric rate consumer group read/commit per seconds
type KafkaConsumerGroupRateMetric struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Group     string    `json:"group,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Rate1     float64   `json:"rate1,omitempty"`
	Rate5     float64   `json:"rate5,omitempty"`
	Rate15    float64   `json:"rate15,omitempty"`
	RateMean  float64   `json:"rate_mean,omitempty"`
	Count     int64     `json:"count,omitempty"`
}

// KafkaTopicPartitions Number of partitions for this Topic
type KafkaTopicPartitions struct {
	Timestamp       time.Time `json:"timestamp,omitempty"`
	Topic           string    `json:"topic,omitempty"`
	PartitionNumber int       `json:"partition_number,omitempty"`
}

// KafkaInSyncReplicas Number of In-Sync Replicas for this Topic/Partition
type KafkaInSyncReplicas struct {
	Timestamp      time.Time `json:"timestamp,omitempty"`
	Topic          string    `json:"topic,omitempty"`
	Partition      int32     `json:"partition,omitempty"`
	InSyncReplicas int       `json:"in_sync_replicas,omitempty"`
}

// KafkaLeaderTopicPartition Leader Broker ID of this Topic/Partition
type KafkaLeaderTopicPartition struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Partition int32     `json:"partition,omitempty"`
	NodeID    int32     `json:"node_id,omitempty"`
}

// KafkaLeaderIsPreferredTopicPartition true if Topic/Partition is using the Preferred Broker
type KafkaLeaderIsPreferredTopicPartition struct {
	Timestamp   time.Time `json:"timestamp,omitempty"`
	Topic       string    `json:"topic,omitempty"`
	Partition   int32     `json:"partition,omitempty"`
	IsPreferred bool      `json:"is_preferred,omitempty"`
}

// KafkaReplicasTopicPartition Number of Replicas for this Topic/Partition
type KafkaReplicasTopicPartition struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Partition int32     `json:"partition,omitempty"`
	Replicas  int       `json:"replicas,omitempty"`
}

// KafkaUnderReplicatedTopicPartition 1 if Topic/Partition is under Replicated
type KafkaUnderReplicatedTopicPartition struct {
	Timestamp         time.Time `json:"timestamp,omitempty"`
	Topic             string    `json:"topic,omitempty"`
	Partition         int32     `json:"partition,omitempty"`
	IsUnderReplicated bool      `json:"is_under_replicated,omitempty"`
}
