package elasticsearch

import (
	"context"
	"flag"
	"os"

	elastic "github.com/olivere/elastic"
	elastic_config "github.com/olivere/elastic/config"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

func init() {
	metrics.RegisterSink("elasticsearch", NewSink)
}

// Sink write metrics to elasticsearch
type Sink struct {
	client *elastic.Client
	index  string

	*common.Sink
}

var (
	elasticsearchURL      = flag.String("elasticsearch-sink-url", "http://localhost:9200", "Elasticsearch sink URL")
	elasticsearchIndex    = flag.String("elasticsearch-sink-index", "metrics", "Elasticsearch index name")
	elasticsearchUsername = flag.String("elasticsearch-username", os.Getenv("SINK_ELASTICSEARCH_USERNAME"), "Elasticsearch username")
	elasticsearchPassword = flag.String("elasticsearch-password", os.Getenv("SINK_ELASTICSEARCH_PASSWORD"), "Elasticsearch password")
)

func (s *Sink) sendMessage(msg interface{}) error {
	_, err := s.client.Index().
		Index(s.index).
		Type("doc").
		BodyJson(msg).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) kafkaMeter(metric metrics.KafkaMeter) error {
	err := s.sendMessage(meter{
		Name:      metric.Name,
		Timestamp: metric.Timestamp,
		Meta:      metric.Meta,
		Rate1:     metric.Rate1(),
		Rate5:     metric.Rate5(),
		Rate15:    metric.Rate15(),
		RateMean:  metric.RateMean(),
		Count:     metric.Count(),
	})
	if err != nil {
		return err
	}
	return nil
}
func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) error {
	err := s.sendMessage(gauge{
		Name:      metric.Name,
		Timestamp: metric.Timestamp,
		Meta:      metric.Meta,
		Value:     metric.Value(),
	})
	if err != nil {
		return err
	}
	return nil
}

// NewSink build elasticsearch sink
func NewSink() (metrics.Sink, error) {
	client, err := elastic.NewClientFromConfig(&elastic_config.Config{
		URL:      *elasticsearchURL,
		Username: *elasticsearchUsername,
		Password: *elasticsearchPassword,
	})
	if err != nil {
		return nil, err
	}
	sink := &Sink{
		client: client,
		index:  *elasticsearchIndex,
		Sink:   common.NewCommonSink(),
	}

	sink.KafkaMeterFunc = sink.kafkaMeter
	sink.KafkaGaugeFunc = sink.kafkaGauge

	sink.Run()

	return sink, nil
}
