package metrics

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
)

// KafkaSource represent kafka cluster source metrics
type KafkaSource struct {
	client sarama.Client

	consumerGroups  []string
	topicPartitions map[string][]int32

	stopCh chan interface{}
	mutex  sync.Mutex
	sink   KafkaSink

	sync.WaitGroup
}

var (
	brokers        = flag.String("source-brokers", "localhost:9092", "Kafka source brokers")
	cacerts        = flag.String("source-ssl-cacerts", "", "Kafka SSL cacerts")
	cert           = flag.String("source-ssl-cert", "", "Kafka SSL cert")
	key            = flag.String("source-ssl-key", "", "Kafka SSL key")
	insecure       = flag.Bool("source-ssl-insecure", false, "Kafka insecure ssl connection")
	username       = flag.String("source-sasl-username", os.Getenv("SOURCE_KAFKA_USERNAME"), "Kafka SASL username")
	password       = flag.String("source-sasl-password", os.Getenv("SOURCE_KAFKA_PASSWORD"), "Kafka SASL password")
	scrapeInterval = flag.Duration("source-scrape-interval", 60*time.Second, "Time beetween scrape kafka metrics")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewKafkaSource build new kafka source scraper
func NewKafkaSource(sink KafkaSink) (*KafkaSource, error) {
	if sink == nil {
		return nil, fmt.Errorf("Unable to fink KafkaSink in config")
	}

	var err error
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	cfg.Net.TLS.Config, cfg.Net.TLS.Enable, err = getTLSConfiguration(*cacerts, *cert, *key, *insecure)
	if err != nil {
		return nil, err
	}
	cfg.Net.SASL.User, cfg.Net.SASL.Password, cfg.Net.SASL.Enable = getSASLConfiguration(*username, *password)
	brokerList := strings.Split(*brokers, ",")

	client, err := sarama.NewClient(brokerList, cfg)
	if err != nil {
		return nil, err
	}

	topicPartitions := make(map[string][]int32)

	return &KafkaSource{
		client:          client,
		sink:            sink,
		topicPartitions: topicPartitions,
	}, nil
}

// Run launch scrape and return stopCh to end scraping
func (s *KafkaSource) Run() chan interface{} {
	s.stopCh = make(chan interface{})
	s.Add(1)
	go func() {
		defer s.Done()
		intervalTicker := time.NewTicker(*scrapeInterval)
		for {
			select {
			case <-intervalTicker.C:
				err := s.fetchMetrics()
				if err != nil {
					logrus.Error(err)
				}
			case <-s.stopCh:
				err := s.Close()
				if err != nil {
					logrus.Error(err)
				}
				return
			}
		}
	}()
	return s.stopCh
}

func (s *KafkaSource) fetchMetrics() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.Add(1)
	defer s.Done()

	err := s.client.RefreshMetadata()
	if err != nil {
		return err
	}
	topics, err := s.client.Topics()
	if err != nil {
		return err
	}
	// Get all topics/partitions
	for _, topic := range topics {
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return err
		}
		s.topicPartitions[topic] = partitions
	}
	// Get all offsets
	lastOffsets := make(map[string]map[int32]int64)
	offsetMetrics := make([]KafkaOffsetMetric, 0)
	for topic, partitions := range s.topicPartitions {
		for _, partition := range partitions {
			oldestOffset, err := s.client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
			newestOffset, err := s.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return err
			}
			offsetMetrics = append(offsetMetrics, KafkaOffsetMetric{
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
	}
	err = s.sink.SendOffsetMetrics(offsetMetrics)
	if err != nil {
		return err
	}

	// Get all groups / offsets
	brokers := s.client.Brokers()
	if len(brokers) < 1 {
		return fmt.Errorf("Unable to connect to brokers %+v", brokers)
	}
	for _, broker := range brokers {
		response, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return err
		}

		for groupID := range response.Groups {
			s.consumerGroups = append(s.consumerGroups, groupID)
		}
	}

	resp, err := brokers[rand.Intn(len(brokers))].DescribeGroups(&sarama.DescribeGroupsRequest{
		Groups: s.consumerGroups,
	})
	if err != nil {
		return err
	}

	consumerGroupAssignedTopicPartition := make(map[string]map[string][]int32)
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

	consumerGroupMetrics := make([]KafkaConsumerGroupOffsetMetric, 0)
	for _, group := range s.consumerGroups {
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
			for partition, offset := range partitions {
				var lastOffset int64
				lastOffsetPartitions, ok := lastOffsets[topic]
				if ok {
					lastOffset = lastOffsetPartitions[partition]
				}

				consumerGroupMetrics = append(consumerGroupMetrics, KafkaConsumerGroupOffsetMetric{
					Group:     group,
					Topic:     topic,
					Partition: partition,
					Offset:    offset.Offset,
					Lag:       lastOffset - offset.Offset,
				})
			}
		}
	}
	err = s.sink.SendConsumerGroupOffsetMetrics(consumerGroupMetrics)
	if err != nil {
		return err
	}

	return nil
}

// Close close kafka client
func (s *KafkaSource) Close() error {
	return s.client.Close()
}

func getTLSConfiguration(caFile string, certFile string, keyFile string, insecure bool) (*tls.Config, bool, error) {
	logrus.Debugf("configure tls %s %s %s %b", caFile, certFile, keyFile, insecure)
	if caFile == "" && (certFile == "" || keyFile == "") {
		return nil, false, nil
	}
	t := &tls.Config{}
	if caFile != "" {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, false, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		t.RootCAs = caCertPool
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, false, err
		}
		t.Certificates = []tls.Certificate{cert}
	}

	t.InsecureSkipVerify = insecure
	logrus.Debugf("TLS config %+v", t)

	return t, true, nil
}

func getSASLConfiguration(username string, password string) (string, string, bool) {
	if username != "" && password != "" {
		return username, password, true
	}
	return "", "", false
}
