package kafka

import (
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

type offsetMetric struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaOffsetMetric
}

type consumerGroupOffsetMetric struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaConsumerGroupOffsetMetric
}

type topicRateMetric struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaTopicRateMetric
}

type consumerGroupRateMetric struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaConsumerGroupRateMetric
}

type topicPartitions struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaTopicPartitions
}

type replicasTopicPartition struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaReplicasTopicPartition
}

type inSyncReplicas struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaInSyncReplicas
}

type leaderTopicPartition struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaLeaderTopicPartition
}

type leaderIsPreferredTopicPartition struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaLeaderIsPreferredTopicPartition
}

type underReplicatedTopicPartition struct {
	Name string `json:"name,omitempty"`
	*metrics.KafkaUnderReplicatedTopicPartition
}
