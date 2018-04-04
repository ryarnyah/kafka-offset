package kafka

import (
	"encoding/json"
	"flag"
	"os"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/util"
)

func init() {
	metrics.RegisterSink("kafka", NewSink)
}

// Sink write metrics to kafka topic
type Sink struct {
	offsetChan    chan []metrics.KafkaOffsetMetric
	groupChan     chan []metrics.KafkaConsumerGroupOffsetMetric
	topicRateChan chan []metrics.KafkaTopicRateMetric
	groupRateChan chan []metrics.KafkaConsumerGroupRateMetric
	stopCh        chan interface{}

	producer sarama.SyncProducer
	topic    string

	wg sync.WaitGroup
}

var (
	kafkaSinkBrokers  = flag.String("kafka-sink-brokers", "localhost:9092", "Kafka sink brokers")
	kafkaSinkCacerts  = flag.String("kafka-sink-ssl-cacerts", "", "Kafka SSL cacerts")
	kafkaSinkCert     = flag.String("kafka-sink-ssl-cert", "", "Kafka SSL cert")
	kafkaSinkKey      = flag.String("kafka-sink-ssl-key", "", "Kafka SSL key")
	kafkaSinkInsecure = flag.Bool("kafka-sink-ssl-insecure", false, "Kafka insecure ssl connection")
	kafkaSinkUsername = flag.String("kafka-sink-sasl-username", os.Getenv("SINK_KAFKA_USERNAME"), "Kafka SASL username")
	kafkaSinkPassword = flag.String("kafka-sink-sasl-password", os.Getenv("SINK_KAFKA_PASSWORD"), "Kafka SASL password")
	kafkaSinkTopic    = flag.String("kafka-sink-topic", "metrics", "Kafka topic to send metrics")
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

// Close close producer and channels
func (sink *Sink) Close() error {
	close(sink.stopCh)
	sink.wg.Wait()
	close(sink.offsetChan)
	close(sink.groupChan)
	close(sink.topicRateChan)
	close(sink.groupRateChan)
	err := sink.producer.Close()
	if err != nil {
		return err
	}
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
					b, err := json.Marshal(ConsumerGroupOffsetMetric{
						Name:      "kafka-consumer-group-offset-metric",
						Timestamp: metric.Timestamp,
						Group:     metric.Group,
						Topic:     metric.Topic,
						Partition: metric.Partition,
						Offset:    metric.Offset,
						Lag:       metric.Lag,
					})
					if err != nil {
						logrus.Error(err)
					} else {
						_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
							Topic: s.topic,
							Value: sarama.ByteEncoder(b),
						})
						if err != nil {
							logrus.Error(err)
						}
					}
				}
			case <-s.stopCh:
				logrus.Info("Kafka ConsumerGroupOffsetMetrics Stoped")
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
					b, err := json.Marshal(OffsetMetric{
						Name:         "kafka-topic-offset-metric",
						Timestamp:    metric.Timestamp,
						Topic:        metric.Topic,
						Partition:    metric.Partition,
						OldestOffset: metric.OldestOffset,
						NewestOffset: metric.NewestOffset,
					})
					if err != nil {
						logrus.Error(err)
					} else {
						_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
							Topic: s.topic,
							Value: sarama.ByteEncoder(b),
						})
						if err != nil {
							logrus.Error(err)
						}
					}
				}
			case <-s.stopCh:
				logrus.Info("Kafka OffsetMetrics Stoped")
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
					b, err := json.Marshal(ConsumerGroupRateMetric{
						Name:      "kafka-consumer-group-rate-metric",
						Timestamp: metric.Timestamp,
						Topic:     metric.Topic,
						Rate1:     metric.Rate1,
						Rate5:     metric.Rate5,
						Rate15:    metric.Rate15,
						RateMean:  metric.RateMean,
						Count:     metric.Count,
					})
					if err != nil {
						logrus.Error(err)
					} else {
						_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
							Topic: s.topic,
							Value: sarama.ByteEncoder(b),
						})
						if err != nil {
							logrus.Error(err)
						}
					}
				}
			case <-s.stopCh:
				logrus.Info("Kafka GroupRateChan Stoped")
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
					b, err := json.Marshal(TopicRateMetric{
						Name:      "kafka-topic-rate-metric",
						Timestamp: metric.Timestamp,
						Topic:     metric.Topic,
						Rate1:     metric.Rate1,
						Rate5:     metric.Rate5,
						Rate15:    metric.Rate15,
						RateMean:  metric.RateMean,
						Count:     metric.Count,
					})
					if err != nil {
						logrus.Error(err)
					} else {
						_, _, err := s.producer.SendMessage(&sarama.ProducerMessage{
							Topic: s.topic,
							Value: sarama.ByteEncoder(b),
						})
						if err != nil {
							logrus.Error(err)
						}
					}
				}
			case <-s.stopCh:
				logrus.Info("Kafka TopicRateChan Stoped")
				return
			}
		}
	}(sink)

}

// NewSink build new kafka sink
func NewSink() (metrics.Sink, error) {
	var err error
	sarama.Logger = logrus.StandardLogger()
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafka-sink"
	cfg.Version = sarama.V0_10_0_0
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

		producer: producer,
		topic:    *kafkaSinkTopic,
	}
	sink.run()
	return sink, nil
}
