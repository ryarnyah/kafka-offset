package metrics

import (
	"fmt"
)

var registeredSinks = map[string](KafkaSinkFunc){}

// KafkaSink reprensent sink for kafka metrics
type KafkaSink interface {
	SendOffsetMetrics() chan<- []KafkaOffsetMetric
	SendConsumerGroupOffsetMetrics() chan<- []KafkaConsumerGroupOffsetMetric
	SendTopicRateMetrics() chan<- []KafkaTopicRateMetric
	SendConsumerGroupRateMetrics() chan<- []KafkaConsumerGroupRateMetric
	Close() error
	// sync.Waitgroup until close
	Wait()
}

// KafkaSinkFunc build new KafkaSink
type KafkaSinkFunc func() (KafkaSink, error)

// RegisterSink register a new sink to be used by kafka-offset
func RegisterSink(name string, f KafkaSinkFunc) {
	registeredSinks[name] = f
}

// New build sink by it's regitred name
func New(name string) (KafkaSink, error) {
	if name == "" {
		return nil, nil
	}
	f, ok := registeredSinks[name]
	if !ok {
		return nil, fmt.Errorf("unknown sink: %s", name)
	}
	return f()
}
