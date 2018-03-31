package sinks

import (
	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

// LogSink default sink use logrus to print metrics
type LogSink struct{}

func init() {
	metrics.RegisterSink("log", NewLogSink)
}

// SendOffsetMetrics print topic/partition metric
func (s *LogSink) SendOffsetMetrics(metrics []metrics.KafkaOffsetMetric) error {
	logrus.Infof("OffsetMetrics %+v", metrics)
	return nil
}

// SendConsumerGroupOffsetMetrics print group/topic/partition metric
func (s *LogSink) SendConsumerGroupOffsetMetrics(metrics []metrics.KafkaConsumerGroupOffsetMetric) error {
	logrus.Infof("ConsumerGroupOffsetMetrics %+v", metrics)
	return nil
}

// Close do nothing
func (s *LogSink) Close() error {
	return nil
}

// Wait do nothing
func (s *LogSink) Wait() {}

// NewLogSink build sink
func NewLogSink() (metrics.KafkaSink, error) {
	return &LogSink{}, nil
}
