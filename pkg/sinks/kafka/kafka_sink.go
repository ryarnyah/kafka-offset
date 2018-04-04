package kafka

import (
	"encoding/json"
	"flag"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
	"github.com/ryarnyah/kafka-offset/pkg/util"
)

func init() {
	metrics.RegisterSink("kafka", NewSink)
}

// Sink write metrics to kafka topic
type Sink struct {
	producer sarama.SyncProducer
	topic    string

	*common.Sink
}

var (
	kafkaSinkBrokers  = flag.String("kafka-sink-brokers", "localhost:9092", "Kafka sink brokers")
	kafkaSinkVersion  = flag.String("kafka-sink-version", sarama.V0_10_2_0.String(), "Kafka sink broker version")
	kafkaSinkCacerts  = flag.String("kafka-sink-ssl-cacerts", "", "Kafka SSL cacerts")
	kafkaSinkCert     = flag.String("kafka-sink-ssl-cert", "", "Kafka SSL cert")
	kafkaSinkKey      = flag.String("kafka-sink-ssl-key", "", "Kafka SSL key")
	kafkaSinkInsecure = flag.Bool("kafka-sink-ssl-insecure", false, "Kafka insecure ssl connection")
	kafkaSinkUsername = flag.String("kafka-sink-sasl-username", os.Getenv("SINK_KAFKA_USERNAME"), "Kafka SASL username")
	kafkaSinkPassword = flag.String("kafka-sink-sasl-password", os.Getenv("SINK_KAFKA_PASSWORD"), "Kafka SASL password")
	kafkaSinkTopic    = flag.String("kafka-sink-topic", "metrics", "Kafka topic to send metrics")
)

// Close close producer and channels
func (s *Sink) closeProducer() error {
	logrus.Info("Closing producer")
	err := s.producer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *Sink) sendMessage(msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: s.topic,
		Value: sarama.ByteEncoder(b),
	})
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

// NewSink build new kafka sink
func NewSink() (metrics.Sink, error) {
	var err error
	version, err := sarama.ParseKafkaVersion(*kafkaSinkVersion)
	if err != nil {
		return nil, err
	}
	sarama.Logger = logrus.StandardLogger()
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafka-sink"
	cfg.Version = version
	cfg.Producer.Return.Successes = true
	cfg.Net.TLS.Config, cfg.Net.TLS.Enable, err = util.GetTLSConfiguration(*kafkaSinkCacerts, *kafkaSinkCert, *kafkaSinkKey, *kafkaSinkInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Net.SASL.User, cfg.Net.SASL.Password, cfg.Net.SASL.Enable = util.GetSASLConfiguration(*kafkaSinkUsername, *kafkaSinkPassword)
	brokerList := strings.Split(*kafkaSinkBrokers, ",")

	producer, err := sarama.NewSyncProducer(brokerList, cfg)
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		producer: producer,
		topic:    *kafkaSinkTopic,
		Sink:     common.NewCommonSink(),
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
	sink.CloseFunc = sink.closeProducer

	sink.Run()

	return sink, nil
}
