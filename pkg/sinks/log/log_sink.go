package log

import (
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
	"github.com/sirupsen/logrus"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	*common.Sink
}

func init() {
	metrics.RegisterSink("log", NewSink)
}

func (s *Sink) kafkaMetrics(m []interface{}) error {
	for _, metric := range m {
		switch metric := metric.(type) {
		case metrics.KafkaMeter:
			s.kafkaMeter(metric)
		case metrics.KafkaGauge:
			s.kafkaGauge(metric)
		}
	}
	return nil
}

func (s *Sink) kafkaMeter(metric metrics.KafkaMeter) {
	logrus.Infof("offsetMetrics %+v", metric)
}
func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) {
	logrus.Infof("consumerGroupOffsetMetrics %+v", metric)
}

// NewSink build sink
func NewSink() (metrics.Sink, error) {
	sink := &Sink{
		Sink: common.NewCommonSink(),
	}

	sink.KafkaMetricsFunc = sink.kafkaMetrics

	sink.Run()

	return sink, nil
}
