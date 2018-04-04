package common

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	OffsetChan                         chan []metrics.KafkaOffsetMetric
	GroupChan                          chan []metrics.KafkaConsumerGroupOffsetMetric
	TopicRateChan                      chan []metrics.KafkaTopicRateMetric
	GroupRateChan                      chan []metrics.KafkaConsumerGroupRateMetric
	TopicPartitionChan                 chan []metrics.KafkaTopicPartitions
	ReplicasTopicPartitionChan         chan []metrics.KafkaReplicasTopicPartition
	InSyncReplicasChan                 chan []metrics.KafkaInSyncReplicas
	LeaderTopicPartitionChan           chan []metrics.KafkaLeaderTopicPartition
	LeaderIsPreferredTopicParitionChan chan []metrics.KafkaLeaderIsPreferredTopicPartition
	UnderReplicatedTopicPartitionChan  chan []metrics.KafkaUnderReplicatedTopicPartition

	OffsetFunc                         func([]metrics.KafkaOffsetMetric) error
	GroupFunc                          func([]metrics.KafkaConsumerGroupOffsetMetric) error
	TopicRateFunc                      func([]metrics.KafkaTopicRateMetric) error
	GroupRateFunc                      func([]metrics.KafkaConsumerGroupRateMetric) error
	TopicPartitionFunc                 func([]metrics.KafkaTopicPartitions) error
	ReplicasTopicPartitionFunc         func([]metrics.KafkaReplicasTopicPartition) error
	InsyncReplicasFunc                 func([]metrics.KafkaInSyncReplicas) error
	LeaderTopicPartitionFunc           func([]metrics.KafkaLeaderTopicPartition) error
	LeaderIsPreferredTopicParitionFunc func([]metrics.KafkaLeaderIsPreferredTopicPartition) error
	UnderReplicatedTopicPartitionFunc  func([]metrics.KafkaUnderReplicatedTopicPartition) error
	CloseFunc                          func() error

	StopCh chan interface{}

	wg sync.WaitGroup
}

// GetOffsetMetricsChan print topic/partition metric
func (s *Sink) GetOffsetMetricsChan() chan<- []metrics.KafkaOffsetMetric {
	return s.OffsetChan
}

// GetConsumerGroupOffsetMetricsChan print group/topic/partition metric
func (s *Sink) GetConsumerGroupOffsetMetricsChan() chan<- []metrics.KafkaConsumerGroupOffsetMetric {
	return s.GroupChan
}

// GetTopicRateMetricsChan return chan
func (s *Sink) GetTopicRateMetricsChan() chan<- []metrics.KafkaTopicRateMetric {
	return s.TopicRateChan
}

// GetConsumerGroupRateMetricsChan return chan
func (s *Sink) GetConsumerGroupRateMetricsChan() chan<- []metrics.KafkaConsumerGroupRateMetric {
	return s.GroupRateChan
}

// GetTopicPartitionChan return chan
func (s *Sink) GetTopicPartitionChan() chan<- []metrics.KafkaTopicPartitions {
	return s.TopicPartitionChan
}

// GetReplicasTopicPartitionChan return chan
func (s *Sink) GetReplicasTopicPartitionChan() chan<- []metrics.KafkaReplicasTopicPartition {
	return s.ReplicasTopicPartitionChan
}

// GetInSyncReplicasChan return chan
func (s *Sink) GetInSyncReplicasChan() chan<- []metrics.KafkaInSyncReplicas {
	return s.InSyncReplicasChan
}

// GetLeaderTopicPartitionChan return chan
func (s *Sink) GetLeaderTopicPartitionChan() chan<- []metrics.KafkaLeaderTopicPartition {
	return s.LeaderTopicPartitionChan
}

// GetLeaderisPreferredTopicPartitionChan return chan
func (s *Sink) GetLeaderisPreferredTopicPartitionChan() chan<- []metrics.KafkaLeaderIsPreferredTopicPartition {
	return s.LeaderIsPreferredTopicParitionChan
}

// GetUnderReplicatedTopicPartitionChan return chan
func (s *Sink) GetUnderReplicatedTopicPartitionChan() chan<- []metrics.KafkaUnderReplicatedTopicPartition {
	return s.UnderReplicatedTopicPartitionChan
}

// Close do nothing
func (s *Sink) Close() error {
	close(s.StopCh)
	s.wg.Wait()
	close(s.OffsetChan)
	close(s.GroupChan)
	close(s.TopicRateChan)
	close(s.GroupRateChan)
	close(s.TopicPartitionChan)
	close(s.ReplicasTopicPartitionChan)
	close(s.InSyncReplicasChan)
	close(s.LeaderTopicPartitionChan)
	close(s.LeaderIsPreferredTopicParitionChan)
	close(s.UnderReplicatedTopicPartitionChan)
	if s.CloseFunc != nil {
		err := s.CloseFunc()
		if err != nil {
			return err
		}
	}
	return nil
}

// NewCommonSink build channels to be used by others sinks
func NewCommonSink() *Sink {
	s := &Sink{}
	s.StopCh = make(chan interface{})
	s.OffsetChan = make(chan []metrics.KafkaOffsetMetric, 1024)
	s.GroupChan = make(chan []metrics.KafkaConsumerGroupOffsetMetric, 1024)
	s.TopicRateChan = make(chan []metrics.KafkaTopicRateMetric, 1024)
	s.GroupRateChan = make(chan []metrics.KafkaConsumerGroupRateMetric, 1024)
	s.TopicPartitionChan = make(chan []metrics.KafkaTopicPartitions, 1024)
	s.ReplicasTopicPartitionChan = make(chan []metrics.KafkaReplicasTopicPartition, 1024)
	s.InSyncReplicasChan = make(chan []metrics.KafkaInSyncReplicas, 1024)
	s.LeaderTopicPartitionChan = make(chan []metrics.KafkaLeaderTopicPartition, 1024)
	s.LeaderIsPreferredTopicParitionChan = make(chan []metrics.KafkaLeaderIsPreferredTopicPartition, 1024)
	s.UnderReplicatedTopicPartitionChan = make(chan []metrics.KafkaUnderReplicatedTopicPartition, 1024)

	return s
}

// Run start consume all channels
func (s *Sink) Run() {
	s.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.UnderReplicatedTopicPartitionChan:
				if s.UnderReplicatedTopicPartitionFunc != nil {
					err := s.UnderReplicatedTopicPartitionFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.LeaderIsPreferredTopicParitionChan:
				if s.LeaderIsPreferredTopicParitionFunc != nil {
					err := s.LeaderIsPreferredTopicParitionFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.LeaderTopicPartitionChan:
				if s.LeaderTopicPartitionFunc != nil {
					err := s.LeaderTopicPartitionFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.InSyncReplicasChan:
				if s.InsyncReplicasFunc != nil {
					err := s.InsyncReplicasFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.ReplicasTopicPartitionChan:
				if s.ReplicasTopicPartitionFunc != nil {
					err := s.ReplicasTopicPartitionFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.TopicPartitionChan:
				if s.TopicPartitionFunc != nil {
					err := s.TopicPartitionFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.GroupChan:
				if s.GroupFunc != nil {
					err := s.GroupFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.OffsetChan:
				if s.OffsetFunc != nil {
					err := s.OffsetFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.GroupRateChan:
				if s.GroupRateFunc != nil {
					err := s.GroupRateFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case metrics := <-s.TopicRateChan:
				if s.TopicRateFunc != nil {
					err := s.TopicRateFunc(metrics)
					if err != nil {
						logrus.Error(err)
					}
				}
			case <-s.StopCh:
				logrus.Info("Sink consume Stopped")
				return
			}
		}
	}(s)
}
