package plugin

import (
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

func TestSendToSink(t *testing.T) {
	client := testSink{
		savedMetrics: make([]any, 0),
	}
	sink := &Sink{
		plugin: &client,
		Sink:   common.NewCommonSink(),
	}
	sink.KafkaMetricsFunc = sink.kafkaMetrics

	sink.Run()

	testMetrics := make([]any, 0)
	testMetrics = append(testMetrics, common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]any),
		},
		Meter: metrics.NilMeter{},
	})
	testMetrics = append(testMetrics, common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]any),
		},
		Gauge: metrics.NilGauge{},
	})

	sink.GetMetricsChan() <- testMetrics

	sink.Close()

	if len(client.savedMetrics) != 2 {
		t.Fail()
	}
}
