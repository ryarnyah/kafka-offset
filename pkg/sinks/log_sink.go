package sinks

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

// LogSink default sink use logrus to print metrics
type LogSink struct {
	offsetChan    chan []metrics.KafkaOffsetMetric
	groupChan     chan []metrics.KafkaConsumerGroupOffsetMetric
	topicRateChan chan []metrics.KafkaTopicRateMetric
	groupRateChan chan []metrics.KafkaConsumerGroupRateMetric
	stopCh        chan interface{}

	wg sync.WaitGroup
}

func init() {
	metrics.RegisterSink("log", NewLogSink)
}

// SendOffsetMetrics print topic/partition metric
func (s *LogSink) SendOffsetMetrics() chan<- []metrics.KafkaOffsetMetric {
	return s.offsetChan
}

// SendConsumerGroupOffsetMetrics print group/topic/partition metric
func (s *LogSink) SendConsumerGroupOffsetMetrics() chan<- []metrics.KafkaConsumerGroupOffsetMetric {
	return s.groupChan
}

// SendTopicRateMetrics return chan
func (s *LogSink) SendTopicRateMetrics() chan<- []metrics.KafkaTopicRateMetric {
	return s.topicRateChan
}

// SendConsumerGroupRateMetrics return chan
func (s *LogSink) SendConsumerGroupRateMetrics() chan<- []metrics.KafkaConsumerGroupRateMetric {
	return s.groupRateChan
}

// Close do nothing
func (s *LogSink) Close() error {
	close(s.stopCh)
	s.wg.Wait()
	close(s.offsetChan)
	close(s.groupChan)
	close(s.topicRateChan)
	close(s.groupRateChan)
	return nil
}

// Wait do nothing
func (s *LogSink) Wait() {
}

// NewLogSink build sink
func NewLogSink() (metrics.KafkaSink, error) {
	offsetChan := make(chan []metrics.KafkaOffsetMetric, 1024)
	groupChan := make(chan []metrics.KafkaConsumerGroupOffsetMetric, 1024)
	topicRateChan := make(chan []metrics.KafkaTopicRateMetric, 1024)
	groupRateChan := make(chan []metrics.KafkaConsumerGroupRateMetric, 1024)
	stopCh := make(chan interface{})

	sink := &LogSink{
		offsetChan:    offsetChan,
		groupChan:     groupChan,
		topicRateChan: topicRateChan,
		groupRateChan: groupRateChan,
		stopCh:        stopCh,
	}
	sink.wg.Add(1)
	go func(s *LogSink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.groupChan:
				logrus.Infof("ConsumerGroupOffsetMetrics %+v", metrics)
			case <-s.stopCh:
				logrus.Info("ConsumerGroupOffsetMetrics Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *LogSink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.offsetChan:
				logrus.Infof("OffsetMetrics %+v", metrics)
			case <-s.stopCh:
				logrus.Info("OffsetMetrics Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *LogSink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.groupRateChan:
				logrus.Infof("GroupRateChan %+v", metrics)
			case <-s.stopCh:
				logrus.Info("GroupRateChan Stoped")
				return
			}
		}
	}(sink)
	sink.wg.Add(1)
	go func(s *LogSink) {
		defer s.wg.Done()
		for {
			select {
			case metrics := <-s.topicRateChan:
				logrus.Infof("TopicRateChan %+v", metrics)
			case <-s.stopCh:
				logrus.Info("TopicRateChan Stoped")
				return
			}
		}
	}(sink)

	return sink, nil
}
