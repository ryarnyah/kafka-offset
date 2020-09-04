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

func (s *Sink) sendMessages(m []interface{}) error {
	bulkRequest := s.client.Bulk()

	for _, msg := range m {
		bulkRequest.Add(
			elastic.NewBulkIndexRequest().
				Index(s.index).
				Type("doc").
				Doc(msg),
		)
	}

	_, err := bulkRequest.
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) kafkaMetrics(m []interface{}) error {
	metricsSnapshot := make([]interface{}, 0)
	for _, metric := range m {
		switch metric := metric.(type) {
		case metrics.KafkaMeter:
			metricsSnapshot = append(metricsSnapshot, s.kafkaMeter(metric))
		case metrics.KafkaGauge:
			metricsSnapshot = append(metricsSnapshot, s.kafkaGauge(metric))
		}
	}
	return s.sendMessages(metricsSnapshot)
}

func (s *Sink) kafkaMeter(metric metrics.KafkaMeter) interface{} {
	return meter{
		Name:      metric.Name,
		Timestamp: metric.Timestamp,
		Meta:      metric.Meta,
		Rate1:     metric.Rate1(),
		Rate5:     metric.Rate5(),
		Rate15:    metric.Rate15(),
		RateMean:  metric.RateMean(),
		Count:     metric.Count(),
	}
}
func (s *Sink) kafkaGauge(metric metrics.KafkaGauge) interface{} {
	return gauge{
		Name:      metric.Name,
		Timestamp: metric.Timestamp,
		Meta:      metric.Meta,
		Value:     metric.Value(),
	}
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

	sink.KafkaMetricsFunc = sink.kafkaMetrics

	sink.Run()

	return sink, nil
}
