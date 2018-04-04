package elasticsearch

import (
	"context"
	"flag"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
	elastic "github.com/olivere/elastic"
	elastic_config "github.com/olivere/elastic/config"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

func init() {
	metrics.RegisterSink("elasticsearch", NewSink)
}

// Sink write metrics to elasticsearch
type Sink struct {
	offsetChan    chan []metrics.KafkaOffsetMetric
	groupChan     chan []metrics.KafkaConsumerGroupOffsetMetric
	topicRateChan chan []metrics.KafkaTopicRateMetric
	groupRateChan chan []metrics.KafkaConsumerGroupRateMetric
	stopCh        chan interface{}

	client *elastic.Client
	index  string

	wg sync.WaitGroup
}

var (
	elasticsearchURL      = flag.String("elasticsearch-sink-url", "http://localhost:9200", "Elasticsearch sink URL")
	elasticsearchIndex    = flag.String("elasticsearch-sink-index", "metrics", "Elasticsearch index name")
	elasticsearchUsername = flag.String("elasticsearch-username", os.Getenv("SINK_ELASTICSEARCH_USERNAME"), "Elasticsearch username")
	elasticsearchPassword = flag.String("elasticsearch-password", os.Getenv("SINK_ELASTICSEARCH_PASSWORD"), "Elasticsearch password")
)

// SendOffsetMetrics return offset channel
func (sink *Sink) SendOffsetMetrics() chan<- []metrics.KafkaOffsetMetric {
	return sink.offsetChan
}

// SendConsumerGroupOffsetMetrics return consumer group offset channel
func (sink *Sink) SendConsumerGroupOffsetMetrics() chan<- []metrics.KafkaConsumerGroupOffsetMetric {
	return sink.groupChan
}

// SendTopicRateMetrics return topic rate offset channel
func (sink *Sink) SendTopicRateMetrics() chan<- []metrics.KafkaTopicRateMetric {
	return sink.topicRateChan
}

// SendConsumerGroupRateMetrics return consumer group rate offset channel
func (sink *Sink) SendConsumerGroupRateMetrics() chan<- []metrics.KafkaConsumerGroupRateMetric {
	return sink.groupRateChan
}

// Close close all channels
func (sink *Sink) Close() error {
	close(sink.stopCh)
	sink.wg.Wait()
	close(sink.offsetChan)
	close(sink.groupChan)
	close(sink.topicRateChan)
	close(sink.groupRateChan)
	return nil
}

// Wait sync.Waitgroup until close
func (sink *Sink) Wait() {

}

func (sink *Sink) run() {
	sink.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.groupChan:
				for _, metric := range metrics {
					m := ConsumerGroupOffsetMetric{
						Name:      "kafka-consumer-group-offset-metric",
						Timestamp: metric.Timestamp,
						Group:     metric.Group,
						Topic:     metric.Topic,
						Partition: metric.Partition,
						Offset:    metric.Offset,
						Lag:       metric.Lag,
					}
					_, err := s.client.Index().
						Index(s.index).
						Type("doc").
						BodyJson(m).
						Do(context.Background())
					if err != nil {
						logrus.Error(err)
					}
				}
			case <-s.stopCh:
				logrus.Info("Elasticsearch ConsumerGroupOffsetMetrics Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.offsetChan:
				for _, metric := range metrics {
					m := OffsetMetric{
						Name:         "kafka-topic-offset-metric",
						Timestamp:    metric.Timestamp,
						Topic:        metric.Topic,
						Partition:    metric.Partition,
						OldestOffset: metric.OldestOffset,
						NewestOffset: metric.NewestOffset,
					}
					_, err := s.client.Index().
						Index(s.index).
						Type("doc").
						BodyJson(m).
						Do(context.Background())
					if err != nil {
						logrus.Error(err)
					}
				}
			case <-s.stopCh:
				logrus.Info("Elasticsearch OffsetMetrics Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.groupRateChan:
				for _, metric := range metrics {
					m := ConsumerGroupRateMetric{
						Name:      "kafka-consumer-group-rate-metric",
						Timestamp: metric.Timestamp,
						Topic:     metric.Topic,
						Rate1:     metric.Rate1,
						Rate5:     metric.Rate5,
						Rate15:    metric.Rate15,
						RateMean:  metric.RateMean,
						Count:     metric.Count,
					}
					_, err := s.client.Index().
						Index(s.index).
						Type("doc").
						BodyJson(m).
						Do(context.Background())
					if err != nil {
						logrus.Error(err)
					}
				}
			case <-s.stopCh:
				logrus.Info("Elasticsearch GroupRateChan Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.topicRateChan:
				for _, metric := range metrics {
					m := TopicRateMetric{
						Name:      "kafka-topic-rate-metric",
						Timestamp: metric.Timestamp,
						Topic:     metric.Topic,
						Rate1:     metric.Rate1,
						Rate5:     metric.Rate5,
						Rate15:    metric.Rate15,
						RateMean:  metric.RateMean,
						Count:     metric.Count,
					}
					_, err := s.client.Index().
						Index(s.index).
						Type("doc").
						BodyJson(m).
						Do(context.Background())
					if err != nil {
						logrus.Error(err)
					}
				}
			case <-s.stopCh:
				logrus.Info("Elasticsearch TopicRateChan Stoped")
				return
			}
		}
	}(sink)

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
	offsetChan := make(chan []metrics.KafkaOffsetMetric, 1024)
	groupChan := make(chan []metrics.KafkaConsumerGroupOffsetMetric, 1024)
	topicRateChan := make(chan []metrics.KafkaTopicRateMetric, 1024)
	groupRateChan := make(chan []metrics.KafkaConsumerGroupRateMetric, 1024)
	stopCh := make(chan interface{})

	sink := &Sink{
		offsetChan:    offsetChan,
		groupChan:     groupChan,
		topicRateChan: topicRateChan,
		groupRateChan: groupRateChan,
		stopCh:        stopCh,

		client: client,
		index:  *elasticsearchIndex,
	}

	sink.run()

	return sink, nil
}
