package influxdb

import (
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

type fakeInfluxDBClient struct {
	points []influxdb.BatchPoints
}

func (fakeInfluxDBClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return time.Second, "", nil
}
func (s *fakeInfluxDBClient) Write(bp influxdb.BatchPoints) error {
	s.points = append(s.points, bp)
	return nil
}
func (fakeInfluxDBClient) Query(q influxdb.Query) (*influxdb.Response, error) {
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
	sink.KafkaMeterFunc = sink.kafkaMeter
	sink.KafkaGaugeFunc = sink.kafkaGauge
	sink.CloseFunc = sink.closeClient

	sink.Run()

	sink.GetMetricsChan() <- common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Meter: metrics.NilMeter{},
	}

	sink.GetMetricsChan() <- common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Gauge: metrics.NilGauge{},
	}

	sink.Close()

	if len(client.points) != 2 {
		t.Fail()
	}
}
