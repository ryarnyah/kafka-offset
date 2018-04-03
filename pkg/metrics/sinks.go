package metrics

import (
	"fmt"
)

var registeredSinks = map[string](SinkFunc){}

// Sink reprensent sink for kafka metrics
type Sink interface {
	SendOffsetMetrics() chan<- []KafkaOffsetMetric
	SendConsumerGroupOffsetMetrics() chan<- []KafkaConsumerGroupOffsetMetric
	SendTopicRateMetrics() chan<- []KafkaTopicRateMetric
	SendConsumerGroupRateMetrics() chan<- []KafkaConsumerGroupRateMetric
	Close() error
	// sync.Waitgroup until close
	Wait()
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
