package metrics

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/ryarnyah/kafka-offset/pkg/util"
)

// KafkaSource represent kafka cluster source metrics
type KafkaSource struct {
	client sarama.Client
	cfg    *sarama.Config

	stopCh chan interface{}
	mutex  sync.Mutex
	sink   Sink

	topicOffsetMeter         map[string]metrics.Meter
	consumerGroupOffsetMeter map[string]map[string]metrics.Meter

	previousTopicOffset         map[string]int64
	previousConsumerGroupOffset map[string]int64

	sync.WaitGroup
}

var (
	sourceBrokers        = flag.String("source-brokers", "localhost:9092", "Kafka source brokers")
	sourceCacerts        = flag.String("source-ssl-cacerts", "", "Kafka SSL cacerts")
	sourceCert           = flag.String("source-ssl-cert", "", "Kafka SSL cert")
	sourceKey            = flag.String("source-ssl-key", "", "Kafka SSL key")
	sourceInsecure       = flag.Bool("source-ssl-insecure", false, "Kafka insecure ssl connection")
	sourceUsername       = flag.String("source-sasl-username", os.Getenv("SOURCE_KAFKA_USERNAME"), "Kafka SASL username")
	sourcePassword       = flag.String("source-sasl-password", os.Getenv("SOURCE_KAFKA_PASSWORD"), "Kafka SASL password")
	sourceScrapeInterval = flag.Duration("source-scrape-interval", 60*time.Second, "Time beetween scrape kafka metrics")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewKafkaSource build new kafka source scraper
func NewKafkaSource(sink Sink) (*KafkaSource, error) {
	if sink == nil {
		return nil, fmt.Errorf("Unable to fink KafkaSink in config")
	}

	var err error
	sarama.Logger = logrus.StandardLogger()
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafka-offset"
	cfg.Version = sarama.V0_10_0_0
	cfg.Net.TLS.Config, cfg.Net.TLS.Enable, err = util.GetTLSConfiguration(*sourceCacerts, *sourceCert, *sourceKey, *sourceInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Net.SASL.User, cfg.Net.SASL.Password, cfg.Net.SASL.Enable = util.GetSASLConfiguration(*sourceUsername, *sourcePassword)
	brokerList := strings.Split(*sourceBrokers, ",")

	client, err := sarama.NewClient(brokerList, cfg)
	if err != nil {
		return nil, err
	}

	topicOffsetMeter := make(map[string]metrics.Meter, 0)
	consumerGroupOffsetMeter := make(map[string]map[string]metrics.Meter)
	previousTopicOffset := make(map[string]int64)
	previousConsumerGroupOffset := make(map[string]int64)

	return &KafkaSource{
		client:                      client,
		sink:                        sink,
		cfg:                         cfg,
		topicOffsetMeter:            topicOffsetMeter,
		consumerGroupOffsetMeter:    consumerGroupOffsetMeter,
		previousTopicOffset:         previousTopicOffset,
		previousConsumerGroupOffset: previousConsumerGroupOffset,
	}, nil
}

// Run launch scrape and return stopCh to end scraping
func (s *KafkaSource) Run() chan interface{} {
	s.stopCh = make(chan interface{})
	s.Add(1)
	go func() {
		defer s.Done()
		intervalTicker := time.NewTicker(*sourceScrapeInterval)
		for {
			select {
			case <-intervalTicker.C:
				err := s.fetchMetrics()
				if err != nil {
					logrus.Error(err)
				}
			case <-s.stopCh:
				return
			}
		}
	}()
	return s.stopCh
}

func (s *KafkaSource) fetchLastOffsetsMetrics(start time.Time) (map[string]map[int32]int64, error) {
	var topicPartitions = make(map[string][]int32)
	topics, err := s.client.Topics()
	if err != nil {
		return nil, err
	}
	// Get all topics/partitions
	for _, topic := range topics {
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		topicPartitions[topic] = partitions
	}

	lastOffsets := make(map[string]map[int32]int64)
	offsetMetrics := make([]KafkaOffsetMetric, 0)
	for topic, partitions := range topicPartitions {
		for _, partition := range partitions {
			oldestOffset, err := s.client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return nil, err
			}
			if err != nil {
				return nil, err
			}
			newestOffset, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}
			offsetMetrics = append(offsetMetrics, KafkaOffsetMetric{
				Timestamp:    start,
				Topic:        topic,
				Partition:    partition,
				NewestOffset: newestOffset,
				OldestOffset: oldestOffset,
			})
			lastOffsetsParition, ok := lastOffsets[topic]
			if !ok {
				lastOffsets[topic] = make(map[int32]int64)
				lastOffsetsParition = lastOffsets[topic]
			}
			lastOffsetsParition[partition] = newestOffset
		}
		// Update topic producer rate
		s.markTopicOffset(topic, lastOffsets[topic])
	}
	s.sink.GetOffsetMetricsChan() <- offsetMetrics

	return lastOffsets, nil
}

func (s *KafkaSource) fetchConsumerGroupMetrics(start time.Time, lastOffsets map[string]map[int32]int64) error {
	var consumerGroups []string

	brokers := s.client.Brokers()
	if len(brokers) < 1 {
		return fmt.Errorf("Unable to connect to brokers %+v", brokers)
	}
	consumerGroupAssignedTopicPartition := make(map[string]map[string][]int32)
	for _, broker := range brokers {
		conn, err := broker.Connected()
		if err != nil {
			return err
		}
		if !conn {
			err := broker.Open(s.cfg)
			if err != nil {
				return err
			}
		}
		response, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return err
		}

		for groupID := range response.Groups {
			consumerGroups = append(consumerGroups, groupID)
		}

		resp, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{
			Groups: consumerGroups,
		})
		if err != nil {
			return err
		}

		for _, groupDescrition := range resp.Groups {
			for _, member := range groupDescrition.Members {
				assign, err := member.GetMemberAssignment()
				if err != nil {
					return err
				}
				topicsParition := make(map[string][]int32)
				if consumerGroupAssignedTopicPartition[groupDescrition.GroupId] != nil {
					topicsParition = consumerGroupAssignedTopicPartition[groupDescrition.GroupId]
				}
				for topic, partitions := range assign.Topics {
					topicsParition[topic] = append(topicsParition[topic], partitions...)
				}
				consumerGroupAssignedTopicPartition[groupDescrition.GroupId] = topicsParition
			}
		}
	}

	consumerGroupMetrics := make([]KafkaConsumerGroupOffsetMetric, 0)
	for _, group := range consumerGroups {
		coordinator, err := s.client.Coordinator(group)
		if err != nil {
			return err
		}

		request := &sarama.OffsetFetchRequest{
			ConsumerGroup: group,
			Version:       1,
		}
		for topic, partitions := range consumerGroupAssignedTopicPartition[group] {
			for _, partition := range partitions {
				request.AddPartition(topic, partition)
			}
		}
		response, err := coordinator.FetchOffset(request)
		if err != nil {
			return err
		}
		for topic, partitions := range response.Blocks {
			lastGroupOffset := make(map[int32]int64)
			for partition, offset := range partitions {
				var lastOffset int64
				lastOffsetPartitions, ok := lastOffsets[topic]
				if ok {
					lastOffset = lastOffsetPartitions[partition]
				}

				consumerGroupMetrics = append(consumerGroupMetrics, KafkaConsumerGroupOffsetMetric{
					Timestamp: start,
					Group:     group,
					Topic:     topic,
					Partition: partition,
					Offset:    offset.Offset,
					Lag:       lastOffset - offset.Offset,
				})
				lastGroupOffset[partition] = offset.Offset
			}
			// Update consumer rate
			s.markConsumerGroupOffset(group, topic, lastGroupOffset)
		}
	}
	s.sink.GetConsumerGroupOffsetMetricsChan() <- consumerGroupMetrics

	return nil
}

func (s *KafkaSource) fetchMetrics() error {
	now := time.Now()
	defer func(startTime time.Time) {
		logrus.Infof("fetchMetrics took %s", time.Since(startTime))
	}(now)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.Add(1)
	defer s.Done()

	err := s.client.RefreshMetadata()
	if err != nil {
		return err
	}
	// Get all offsets
	lastOffsets, err := s.fetchLastOffsetsMetrics(now)
	if err != nil {
		return err
	}

	// Get all groups / offsets
	err = s.fetchConsumerGroupMetrics(now, lastOffsets)
	if err != nil {
		return err
	}

	topicRateSnap := make([]KafkaTopicRateMetric, 0)
	for topic, meter := range s.topicOffsetMeter {
		topicRateSnap = append(topicRateSnap, KafkaTopicRateMetric{
			Timestamp: now,
			Topic:     topic,
			Rate1:     meter.Rate1(),
			Rate5:     meter.Rate5(),
			Rate15:    meter.Rate15(),
			RateMean:  meter.RateMean(),
			Count:     meter.Count(),
		})
	}

	consumerGroupRateSnap := make([]KafkaConsumerGroupRateMetric, 0)
	for group, topics := range s.consumerGroupOffsetMeter {
		for topic, meter := range topics {
			consumerGroupRateSnap = append(consumerGroupRateSnap, KafkaConsumerGroupRateMetric{
				Timestamp: now,
				Group:     group,
				Topic:     topic,
				Rate1:     meter.Rate1(),
				Rate5:     meter.Rate5(),
				Rate15:    meter.Rate15(),
				RateMean:  meter.RateMean(),
				Count:     meter.Count(),
			})
		}
	}
	s.sink.GetTopicRateMetricsChan() <- topicRateSnap
	s.sink.GetConsumerGroupRateMetricsChan() <- consumerGroupRateSnap
	return nil
}

// Close close kafka client
func (s *KafkaSource) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, broker := range s.client.Brokers() {
		if connected, _ := broker.Connected(); connected {
			if err := broker.Close(); err != nil {
				logrus.Errorf("Error closing broker %d : %s", broker.ID(), err)
			}
		}
	}

	for _, meter := range s.topicOffsetMeter {
		meter.Stop()
	}

	for _, topicMeter := range s.consumerGroupOffsetMeter {
		for _, meter := range topicMeter {
			meter.Stop()
		}
	}

	return s.client.Close()
}

func (s *KafkaSource) markTopicOffset(topic string, partitionOffsets map[int32]int64) {
	var offset int64

	// Sum offsets
	for _, o := range partitionOffsets {
		offset += o
	}

	previousOffset, ok := s.previousTopicOffset[topic]
	if ok {
		meter, ok := s.topicOffsetMeter[topic]
		if !ok {
			s.topicOffsetMeter[topic] = metrics.NewMeter()
			meter = s.topicOffsetMeter[topic]
		}
		meter.Mark(offset - previousOffset)
	}
	s.previousTopicOffset[topic] = offset
}

func (s *KafkaSource) markConsumerGroupOffset(group, topic string, partitionOffsets map[int32]int64) {
	var offset int64

	// Sum offsets
	for _, o := range partitionOffsets {
		offset += o
	}

	previousOffset, ok := s.previousConsumerGroupOffset[group+"_"+topic]
	if ok {
		topics, ok := s.consumerGroupOffsetMeter[group]
		if !ok {
			s.consumerGroupOffsetMeter[group] = make(map[string]metrics.Meter)
			topics = s.consumerGroupOffsetMeter[group]
		}
		meter, ok := topics[topic]
		if !ok {
			topics[topic] = metrics.NewMeter()
			meter = topics[topic]
		}
		meter.Mark(offset - previousOffset)
	}
	s.previousConsumerGroupOffset[group+"_"+topic] = offset
}
