package influxdb

import (
	"flag"
	"fmt"
	"log"
	"os"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

func init() {
	metrics.RegisterSink("influxdb", NewSink)
}

var (
	influxDBAddr     = flag.String("influxdb-addr", "http://localhost:8086", "Hostname of influxdb")
	influxDBUsername = flag.String("influxdb-username", os.Getenv("INFLUXDB_USERNAME"), "Influxdb username")
	influxDBPassword = flag.String("influxdb-password", os.Getenv("INFLUXDB_PASSWORD"), "Influxdb user password")
	influxDBDatabase = flag.String("influxdb-database", "metrics", "Influxdb database")
)

// Sink write metrics to kafka topic
type Sink struct {
	client influxdb.Client

	*common.Sink
}

func (s *Sink) closeClient() error {
	return s.client.Close()
}

func (s *Sink) kafkaMeter(metric metrics.KafkaMeter) error {
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  *influxDBDatabase,
		Precision: "s",
	})
	if err != nil {
		return err
	}
	tags := make(map[string]string, len(metric.Meta))
	for k, v := range metric.Meta {
		tags[k] = fmt.Sprintf("%v", v)
	}
	fields := map[string]interface{}{
		"count":     metric.Count(),
		"rate1":     metric.Rate1(),
		"rate5":     metric.Rate5(),
		"rate15":    metric.Rate15(),
		"rate_mean": metric.RateMean(),
	}
	pt, err := influxdb.NewPoint(metric.Name, tags, fields, metric.Timestamp)
	if err != nil {
		return err
	}
	bp.AddPoint(pt)
	if err := s.client.Write(bp); err != nil {
		return err
	}
	return nil
}

func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) error {
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  *influxDBDatabase,
		Precision: "s",
	})
	if err != nil {
		return err
	}
	tags := make(map[string]string, len(metric.Meta))
	for k, v := range metric.Meta {
		tags[k] = fmt.Sprintf("%v", v)
	}
	fields := map[string]interface{}{
		"value": metric.Value(),
	}
	pt, err := influxdb.NewPoint(metric.Name, tags, fields, metric.Timestamp)
	if err != nil {
		return err
	}
	bp.AddPoint(pt)
	if err := s.client.Write(bp); err != nil {
		return err
	}
	return nil
}

// NewSink build new kafka sink
func NewSink() (metrics.Sink, error) {
	c, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     *influxDBAddr,
		Username: *influxDBUsername,
		Password: *influxDBPassword,
	})
	if err != nil {
		log.Fatal(err)
	}

	sink := &Sink{
		Sink:   common.NewCommonSink(),
		client: c,
	}

	sink.KafkaMeterFunc = sink.kafkaMeter
	sink.KafkaGaugeFunc = sink.kafkaGauge
	sink.CloseFunc = sink.closeClient

	sink.Run()

	return sink, nil
}
