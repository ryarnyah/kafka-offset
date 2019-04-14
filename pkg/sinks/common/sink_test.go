package common

import (
	"testing"
	"time"

	go_metrics "github.com/rcrowley/go-metrics"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
)

func TestSendToSink(t *testing.T) {
	gauges := make([]metrics.KafkaGauge, 0)
	meters := make([]metrics.KafkaMeter, 0)

	sink := NewCommonSink()
	sink.KafkaGaugeFunc = func(metric metrics.KafkaGauge) error {
		gauges = append(gauges, metric)
		return nil
	}

	sink.KafkaMeterFunc = func(metric metrics.KafkaMeter) error {
		meters = append(meters, metric)
		return nil
	}

	sink.Run()

	sink.GetMetricsChan() <- common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Meter: go_metrics.NilMeter{},
	}

	sink.GetMetricsChan() <- common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Gauge: go_metrics.NilGauge{},
	}

	sink.Close()

	if len(meters) != 1 {
		t.Fail()
	}
	if len(gauges) != 1 {
		t.Fail()
	}
}
