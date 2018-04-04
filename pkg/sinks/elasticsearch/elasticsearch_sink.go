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

func (s *Sink) offsetMetrics(metrics []metrics.KafkaOffsetMetric) error {
	for _, metric := range metrics {
		m := offsetMetric{
			Name:              "kafka-topic-offset-metric",
			KafkaOffsetMetric: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) consumerGroupOffsetMetrics(metrics []metrics.KafkaConsumerGroupOffsetMetric) error {
	for _, metric := range metrics {
		m := consumerGroupOffsetMetric{
			Name: "kafka-consumer-group-offset-metric",
			KafkaConsumerGroupOffsetMetric: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) topicRateMetrics(metrics []metrics.KafkaTopicRateMetric) error {
	for _, metric := range metrics {
		m := topicRateMetric{
			Name:                 "kafka-topic-rate-metric",
			KafkaTopicRateMetric: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) consumerGroupRateMetrics(metrics []metrics.KafkaConsumerGroupRateMetric) error {
	for _, metric := range metrics {
		m := consumerGroupRateMetric{
			Name: "kafka-consumer-group-rate-metric",
			KafkaConsumerGroupRateMetric: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) topicPartitionMetrics(metrics []metrics.KafkaTopicPartitions) error {
	for _, metric := range metrics {
		m := topicPartitions{
			Name:                 "kafka-topic-partition-metric",
			KafkaTopicPartitions: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) replicasTopicPartitionMetrics(metrics []metrics.KafkaReplicasTopicPartition) error {
	for _, metric := range metrics {
		m := replicasTopicPartition{
			Name: "kafka-replicas-topic-partition-metric",
			KafkaReplicasTopicPartition: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) inSyncReplicasMetrics(metrics []metrics.KafkaInSyncReplicas) error {
	for _, metric := range metrics {
		m := inSyncReplicas{
			Name:                "kafka-in-sync-replicas-metric",
			KafkaInSyncReplicas: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) leaderTopicPartitionMetrics(metrics []metrics.KafkaLeaderTopicPartition) error {
	for _, metric := range metrics {
		m := leaderTopicPartition{
			Name: "kafka-leader-topic-partition-metric",
			KafkaLeaderTopicPartition: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) leaderIsPreferredTopicPartitionMetrics(metrics []metrics.KafkaLeaderIsPreferredTopicPartition) error {
	for _, metric := range metrics {
		m := leaderIsPreferredTopicPartition{
			Name: "kafka-leader-is-preferred-topic-partition-metric",
			KafkaLeaderIsPreferredTopicPartition: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Sink) underReplicatedTopicPartitionMetrics(metrics []metrics.KafkaUnderReplicatedTopicPartition) error {
	for _, metric := range metrics {
		m := underReplicatedTopicPartition{
			Name: "kafka-under-replicated-topic-partition-metric",
			KafkaUnderReplicatedTopicPartition: &metric,
		}
		err := s.sendMessage(m)
		if err != nil {
			return err
		}
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

	sink.OffsetFunc = sink.offsetMetrics
	sink.GroupFunc = sink.consumerGroupOffsetMetrics
	sink.TopicRateFunc = sink.topicRateMetrics
	sink.GroupRateFunc = sink.consumerGroupRateMetrics
	sink.TopicPartitionFunc = sink.topicPartitionMetrics
	sink.ReplicasTopicPartitionFunc = sink.replicasTopicPartitionMetrics
	sink.InsyncReplicasFunc = sink.inSyncReplicasMetrics
	sink.LeaderTopicPartitionFunc = sink.leaderTopicPartitionMetrics
	sink.LeaderIsPreferredTopicParitionFunc = sink.leaderIsPreferredTopicPartitionMetrics
	sink.UnderReplicatedTopicPartitionFunc = sink.underReplicatedTopicPartitionMetrics

	sink.Run()

	return sink, nil
}
