package collectd

import (
	"flag"
	"fmt"
	"os"

	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

func init() {
	metrics.RegisterSink("collectd", NewSink)
}

var (
	collectdHostname = flag.String("collectd-hostname", os.Getenv("COLLECTD_HOSTNAME"), "Hostname for collectd plugin")
	collectdInterval = flag.String("collectd-interval", os.Getenv("COLLECTD_INTERVAL"), "Collectd interval")
)

// Sink write metrics to kafka topic
type Sink struct {
	*common.Sink
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
	fmt.Printf("PUTVAL %s/kafka/%s-%s interval=%s %d:%d,%d:%f,%d:%f,%d:%f,%d:%f\n", *collectdHostname, metric.Name, metric.Key, *collectdInterval,
		metric.Timestamp.Unix(), metric.Count(),
		metric.Timestamp.Unix(), metric.Rate1(),
		metric.Timestamp.Unix(), metric.Rate5(),
		metric.Timestamp.Unix(), metric.Rate15(),
		metric.Timestamp.Unix(), metric.RateMean(),
	)
}

func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) {
	fmt.Printf("PUTVAL %s/kafka/%s-%s interval=%s %d:%d\n", *collectdHostname, metric.Name, metric.Key, *collectdInterval, metric.Timestamp.Unix(), metric.Value())
}

// NewSink build new kafka sink
func NewSink() (metrics.Sink, error) {
	sink := &Sink{
		Sink: common.NewCommonSink(),
	}
	sink.KafkaMetricsFunc = sink.kafkaMetrics

	sink.Run()

	return sink, nil
}
