package influxdb

import (
	"context"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

type fakeInfluxDBClient struct {
	points []*influxdb.Point
}

func (fakeInfluxDBClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return time.Second, "", nil
}
func (s *fakeInfluxDBClient) Write(bp influxdb.BatchPoints) error {
	s.points = append(s.points, bp.Points()...)
	return nil
}
func (fakeInfluxDBClient) Query(q influxdb.Query) (*influxdb.Response, error) {
	return nil, nil
}
func (fakeInfluxDBClient) QueryCtx(ctx context.Context, q influxdb.Query) (*influxdb.Response, error) {
	return nil, nil
}
func (fakeInfluxDBClient) QueryAsChunk(q influxdb.Query) (*influxdb.ChunkedResponse, error) {
	return nil, nil
}

func (fakeInfluxDBClient) Close() error {
	return nil
}

func (s *fakeInfluxDBClient) CleanUp() {
	s.points = nil
}
func TestSendToSink(t *testing.T) {
	client := fakeInfluxDBClient{}
	defer client.CleanUp()
	sink := &Sink{
		client: &client,
		Sink:   common.NewCommonSink(),
	}
	sink.KafkaMetricsFunc = sink.kafkaMetrics
	sink.CloseFunc = sink.closeClient

	sink.Run()

	testMetrics := make([]interface{}, 0)
	testMetrics = append(testMetrics, common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Meter: metrics.NilMeter{},
	})
	testMetrics = append(testMetrics, common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Gauge: metrics.NilGauge{},
	})

	sink.GetMetricsChan() <- testMetrics

	sink.Close()

	if len(client.points) != 2 {
		t.Fail()
	}
}
