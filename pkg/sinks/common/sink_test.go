package common

import (
	"testing"
	"time"

	go_metrics "github.com/rcrowley/go-metrics"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

func TestSendToSink(t *testing.T) {
	metricsResult := make([]interface{}, 0)

	sink := NewCommonSink()
	sink.KafkaMetricsFunc = func(metric []interface{}) error {
		metricsResult = metric
		return nil
	}

	sink.Run()

	testMetrics := make([]interface{}, 0)
	testMetrics = append(testMetrics, metrics.KafkaMeter{
		BaseMetric: metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Meter: go_metrics.NilMeter{},
	})
	testMetrics = append(testMetrics, metrics.KafkaGauge{
		BaseMetric: metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Gauge: go_metrics.NilGauge{},
	})
	sink.GetMetricsChan() <- testMetrics

	sink.Close()

	if len(metricsResult) != 2 {
		t.Fail()
	}
}
