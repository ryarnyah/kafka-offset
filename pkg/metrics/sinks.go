package metrics

import (
	"fmt"
)

var registeredSinks = map[string](SinkFunc){}

// Sink reprensent sink for kafka metrics
type Sink interface {
	GetOffsetMetricsChan() chan<- []KafkaOffsetMetric
	GetConsumerGroupOffsetMetricsChan() chan<- []KafkaConsumerGroupOffsetMetric
	GetTopicRateMetricsChan() chan<- []KafkaTopicRateMetric
	GetConsumerGroupRateMetricsChan() chan<- []KafkaConsumerGroupRateMetric
	GetTopicPartitionChan() chan<- []KafkaTopicPartitions
	GetReplicasTopicPartitionChan() chan<- []KafkaReplicasTopicPartition
	GetInSyncReplicasChan() chan<- []KafkaInSyncReplicas
	GetLeaderTopicPartitionChan() chan<- []KafkaLeaderTopicPartition
	GetLeaderisPreferredTopicPartitionChan() chan<- []KafkaLeaderIsPreferredTopicPartition
	GetUnderReplicatedTopicPartitionChan() chan<- []KafkaUnderReplicatedTopicPartition
	Close() error
}

// SinkFunc build new KafkaSink
type SinkFunc func() (Sink, error)

// RegisterSink register a new sink to be used by kafka-offset
func RegisterSink(name string, f SinkFunc) {
	registeredSinks[name] = f
}

// New build sink by it's regitred name
func New(name string) (Sink, error) {
	if name == "" {
		return nil, nil
	}
	f, ok := registeredSinks[name]
	if !ok {
		return nil, fmt.Errorf("unknown sink: %s", name)
	}
	return f()
}
