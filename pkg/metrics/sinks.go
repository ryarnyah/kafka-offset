package metrics

import (
	"fmt"
)

var registeredSinks = map[string](KafkaSinkFunc){}

// KafkaSink reprensent sink for kafka metrics
type KafkaSink interface {
	SendOffsetMetrics([]KafkaOffsetMetric) error
	SendConsumerGroupOffsetMetrics([]KafkaConsumerGroupOffsetMetric) error
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
