package log

import (
	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	*common.Sink
}

func init() {
	metrics.RegisterSink("log", NewSink)
}

func (s *Sink) kafkaMeter(metric metrics.KafkaMeter) error {
	logrus.Infof("offsetMetrics %+v", metric)
	return nil
}
func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) error {
	logrus.Infof("consumerGroupOffsetMetrics %+v", metric)
	return nil
}

// NewSink build sink
func NewSink() (metrics.Sink, error) {
	sink := &Sink{
		Sink: common.NewCommonSink(),
	}

	sink.KafkaMeterFunc = sink.kafkaMeter
	sink.KafkaGaugeFunc = sink.kafkaGauge

	sink.Run()

	return sink, nil
}
