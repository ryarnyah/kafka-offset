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
	metrics "github.com/rcrowley/go-metrics"
	"github.com/ryarnyah/kafka-offset/pkg/util"
	"github.com/sirupsen/logrus"
)

// KafkaSource represent kafka cluster source metrics
type KafkaSource struct {
	client sarama.Client
	cfg    *sarama.Config

	stopCh chan interface{}
	mutex  sync.Mutex
	sink   Sink

	kafkaRegistry metrics.Registry

	previousTopicOffset         map[string]int64
	previousConsumerGroupOffset map[string]int64

	sync.WaitGroup
}

var (
	sourceBrokers        = flag.String("source-brokers", "localhost:9092", "Kafka source brokers")
	sourceKafkaVersion   = flag.String("source-kafka-version", sarama.V0_10_2_0.String(), "Kafka source broker version")
	sourceCacerts        = flag.String("source-ssl-cacerts", "", "Kafka SSL cacerts")
	sourceCert           = flag.String("source-ssl-cert", "", "Kafka SSL cert")
	sourceKey            = flag.String("source-ssl-key", "", "Kafka SSL key")
	sourceInsecure       = flag.Bool("source-ssl-insecure", false, "Kafka insecure ssl connection")
	sourceUsername       = flag.String("source-sasl-username", os.Getenv("SOURCE_KAFKA_USERNAME"), "Kafka SASL username")
	sourcePassword       = flag.String("source-sasl-password", os.Getenv("SOURCE_KAFKA_PASSWORD"), "Kafka SASL password")
	sourceScrapeInterval = flag.Duration("source-scrape-interval", 60*time.Second, "Time beetween scrape kafka metrics")
	sinkProduceInterval  = flag.Duration("sink-produce-interval", 60*time.Second, "Time beetween metrics production")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewKafkaSource build new kafka source scraper
func NewKafkaSource(sink Sink) (*KafkaSource, error) {
	if sink == nil {
		return nil, fmt.Errorf("unable to fink KafkaSink in config")
	}

	var err error
	version, err := sarama.ParseKafkaVersion(*sourceKafkaVersion)
	if err != nil {
		return nil, err
	}
	sarama.Logger = logrus.StandardLogger()
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafka-offset"
	cfg.Version = version
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

	previousTopicOffset := make(map[string]int64)
	previousConsumerGroupOffset := make(map[string]int64)

	return &KafkaSource{
		client:                      client,
		sink:                        sink,
		cfg:                         cfg,
		kafkaRegistry:               metrics.NewRegistry(),
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
	s.Add(1)
	go func() {
		defer s.Done()
		intervalTicker := time.NewTicker(*sinkProduceInterval)
		for {
			select {
			case <-intervalTicker.C:
				err := s.produceMetrics()
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
		topicPartitionsMetric := s.getOrRegister(fmt.Sprintf("kafka_topic_partition_%s", topic),
			func() interface{} {
				return NewKafkaGauge("kafka_topic_partition", topic, map[string]interface{}{
					"topic":     topic,
					"timestamp": start,
				})
			}).(metrics.Gauge)
		topicPartitionsMetric.Update(int64(len(partitions)))
	}

	lastOffsets := make(map[string]map[int32]int64)
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
			offsetNewestMetric := s.getOrRegister(fmt.Sprintf("kafka_topic_partition_offset_newest_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_topic_partition_offset_newest", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			offsetNewestMetric.Update(newestOffset)
			offsetOldestMetric := s.getOrRegister(fmt.Sprintf("kafka_topic_partition_offset_oldest_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_topic_partition_offset_oldest", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			offsetOldestMetric.Update(oldestOffset)
			lastOffsetsParition, ok := lastOffsets[topic]
			if !ok {
				lastOffsets[topic] = make(map[int32]int64)
				lastOffsetsParition = lastOffsets[topic]
			}
			lastOffsetsParition[partition] = newestOffset

			leader, err := s.client.Leader(topic, partition)
			if err != nil {
				return nil, err
			}
			if leader != nil {
				leaderTopicPartitionMetric := s.getOrRegister(fmt.Sprintf("kafka_leader_topic_partition_%s_%d", topic, partition),
					func() interface{} {
						return NewKafkaGauge("kafka_leader_topic_partition", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
							"topic":     topic,
							"partition": partition,
							"timestamp": start,
						})
					}).(metrics.Gauge)
				leaderTopicPartitionMetric.Update(int64(leader.ID()))
			}

			replicas, err := s.client.Replicas(topic, partition)
			if err != nil {
				return nil, err
			}
			replicasTopicPartitionMetric := s.getOrRegister(fmt.Sprintf("kafka_replicas_topic_partition_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_replicas_topic_partition", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			replicasTopicPartitionMetric.Update(int64(len(replicas)))
			inSyncReplicas, err := s.client.InSyncReplicas(topic, partition)
			if err != nil {
				return nil, err
			}
			inSyncReplicasMetric := s.getOrRegister(fmt.Sprintf("kafka_in_sync_replicas_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_in_sync_replicas", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			inSyncReplicasMetric.Update(int64(len(inSyncReplicas)))
			leaderIsPreferredTopicPartitionMetric := s.getOrRegister(fmt.Sprintf("kafka_leader_is_preferred_topic_partition_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_leader_is_preferred_topic_partition", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			if leader != nil && replicas != nil && len(replicas) > 0 && leader.ID() == replicas[0] {
				leaderIsPreferredTopicPartitionMetric.Update(1)
			} else {
				leaderIsPreferredTopicPartitionMetric.Update(0)
			}
			underReplicatedTopicPartitionMetric := s.getOrRegister(fmt.Sprintf("kafka_under_replicated_topic_partition_%s_%d", topic, partition),
				func() interface{} {
					return NewKafkaGauge("kafka_under_replicated_topic_partition", fmt.Sprintf("%s-%d", topic, partition), map[string]interface{}{
						"topic":     topic,
						"partition": partition,
						"timestamp": start,
					})
				}).(metrics.Gauge)
			if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
				underReplicatedTopicPartitionMetric.Update(1)
			} else {
				underReplicatedTopicPartitionMetric.Update(0)
			}
		}
		// Update topic producer rate
		s.markTopicOffset(topic, lastOffsets[topic])
	}

	return lastOffsets, nil
}

func (s *KafkaSource) fetchConsumerGroupMetrics(start time.Time, lastOffsets map[string]map[int32]int64) error {
	var consumerGroups []string

	brokers := s.client.Brokers()
	if len(brokers) < 1 {
		return fmt.Errorf("unable to connect to brokers %+v", brokers)
	}
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
	}

	for _, group := range consumerGroups {
		topicsParitions := make(map[string][]int32)

		coordinator, err := s.client.Coordinator(group)
		if err != nil {
			return err
		}

		resp, err := coordinator.DescribeGroups(&sarama.DescribeGroupsRequest{
			Groups: []string{group},
		})
		if err != nil {
			return err
		}

		for _, groupDescrition := range resp.Groups {
			// Check if group is a consumer group (Cf ConsumerProtocol.PROTOCOL_TYPE) and if group is stable (no reblancing)
			if groupDescrition.State == "Stable" && groupDescrition.ProtocolType == "consumer" {
				for _, member := range groupDescrition.Members {
					assign, err := member.GetMemberAssignment()
					if err != nil {
						return err
					}
					for topic, partitions := range assign.Topics {
						topicsParitions[topic] = append(topicsParitions[topic], partitions...)
					}
				}
			}
		}

		request := &sarama.OffsetFetchRequest{
			ConsumerGroup: group,
			Version:       1,
		}
		for topic, partitions := range topicsParitions {
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

				consumerGroupLag := s.getOrRegister(fmt.Sprintf("kafka_consumer_group_lag_%s_%s_%d", group, topic, partition),
					func() interface{} {
						return NewKafkaGauge("kafka_consumer_group_lag", fmt.Sprintf("%s-%s-%d", group, topic, partition), map[string]interface{}{
							"group":     group,
							"topic":     topic,
							"partition": partition,
							"timestamp": start,
						})
					}).(metrics.Gauge)
				consumerGroupLag.Update(lastOffset - offset.Offset)
				consumerGroupLastOffset := s.getOrRegister(fmt.Sprintf("kafka_consumer_group_latest_offset_%s_%s_%d", group, topic, partition),
					func() interface{} {
						return NewKafkaGauge("kafka_consumer_group_latest_offset", fmt.Sprintf("%s-%s-%d", group, topic, partition), map[string]interface{}{
							"group":     group,
							"topic":     topic,
							"partition": partition,
							"timestamp": start,
						})
					}).(metrics.Gauge)
				consumerGroupLastOffset.Update(offset.Offset)
				lastGroupOffset[partition] = offset.Offset
			}
			// Update consumer rate
			s.markConsumerGroupOffset(group, topic, lastGroupOffset)
		}
	}

	return nil
}

// Build a meter or a gauge only if not already created
func (s *KafkaSource) getOrRegister(name string, newMetric func() interface{}) interface{} {
	m := s.kafkaRegistry.Get(name)
	if m == nil {
		m = newMetric()
		_ = s.kafkaRegistry.Register(name, m)
	}
	return m
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
	return nil
}

func (s *KafkaSource) produceMetrics() error {
	metricsSnapshot := make([]interface{}, 0)
	s.kafkaRegistry.Each(func(name string, metric interface{}) {
		switch metric := metric.(type) {
		case metrics.Meter:
			metricsSnapshot = append(metricsSnapshot, metric.Snapshot())
		case metrics.Gauge:
			metricsSnapshot = append(metricsSnapshot, metric.Snapshot())
		}
	})
	s.sink.GetMetricsChan() <- metricsSnapshot

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

	s.kafkaRegistry.UnregisterAll()

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
		topicRate := s.getOrRegister(fmt.Sprintf("kafka_topic_rate_%s", topic),
			func() interface{} {
				return NewKafkaMeter("kafka_topic_rate", topic, map[string]interface{}{
					"topic": topic,
				})
			}).(metrics.Meter)
		topicRate.Mark(offset - previousOffset)
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
		consumerGroupRate := s.getOrRegister(fmt.Sprintf("kafka_consumer_group_rate_%s_%s", group, topic),
			func() interface{} {
				return NewKafkaMeter("kafka_consumer_group_rate", fmt.Sprintf("%s-%s", group, topic), map[string]interface{}{
					"group": group,
					"topic": topic,
				})
			}).(metrics.Meter)
		consumerGroupRate.Mark(offset - previousOffset)
	}
	s.previousConsumerGroupOffset[group+"_"+topic] = offset
}
