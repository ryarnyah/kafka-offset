package log

import (
	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	*common.Sink
}

func init() {
	metrics.RegisterSink("log", NewSink)
}

func (s *Sink) offsetMetrics(metrics []metrics.KafkaOffsetMetric) error {
	logrus.Infof("offsetMetrics %+v", metrics)
	return nil
}
func (s *Sink) consumerGroupOffsetMetrics(metrics []metrics.KafkaConsumerGroupOffsetMetric) error {
	logrus.Infof("consumerGroupOffsetMetrics %+v", metrics)
	return nil
}
func (s *Sink) topicRateMetrics(metrics []metrics.KafkaTopicRateMetric) error {
	logrus.Infof("topicRateMetrics %+v", metrics)
	return nil
}
func (s *Sink) consumerGroupRateMetrics(metrics []metrics.KafkaConsumerGroupRateMetric) error {
	logrus.Infof("consumerGroupRateMetrics %+v", metrics)
	return nil
}
func (s *Sink) topicPartitionMetrics(metrics []metrics.KafkaTopicPartitions) error {
	logrus.Infof("topicPartitionMetrics %+v", metrics)
	return nil
}
func (s *Sink) replicasTopicPartitionMetrics(metrics []metrics.KafkaReplicasTopicPartition) error {
	logrus.Infof("replicasTopicPartitionMetrics %+v", metrics)
	return nil
}
func (s *Sink) inSyncReplicasMetrics(metrics []metrics.KafkaInSyncReplicas) error {
	logrus.Infof("inSyncReplicasMetrics %+v", metrics)
	return nil
}
func (s *Sink) leaderTopicPartitionMetrics(metrics []metrics.KafkaLeaderTopicPartition) error {
	logrus.Infof("leaderTopicPartitionMetrics %+v", metrics)
	return nil
}
func (s *Sink) leaderIsPreferredTopicPartitionMetrics(metrics []metrics.KafkaLeaderIsPreferredTopicPartition) error {
	logrus.Infof("leaderIsPreferredTopicPartitionMetrics %+v", metrics)
	return nil
}
func (s *Sink) underReplicatedTopicPartitionMetrics(metrics []metrics.KafkaUnderReplicatedTopicPartition) error {
	logrus.Infof("underReplicatedTopicPartitionMetrics %+v", metrics)
	return nil
}

// NewSink build sink
func NewSink() (metrics.Sink, error) {
	sink := &Sink{
		Sink: common.NewCommonSink(),
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
