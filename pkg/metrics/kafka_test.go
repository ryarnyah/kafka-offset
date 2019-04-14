package metrics

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	go_metrics "github.com/rcrowley/go-metrics"
	metrics "github.com/rcrowley/go-metrics"
)

func TestFetchLastOffsetsMetrics(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	replicas := []int32{seedBroker.BrokerID()}

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, seedBroker.BrokerID(), replicas, replicas, []int32{}, sarama.ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, seedBroker.BrokerID(), replicas, replicas, []int32{}, sarama.ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := sarama.NewConfig()
	config.Metadata.Retry.Max = 0
	config.Version = sarama.V0_9_0_0
	c, err := sarama.NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	previousTopicOffset := make(map[string]int64)
	previousConsumerGroupOffset := make(map[string]int64)

	sink := fakeSink{
		metrics: make([]interface{}, 0),
	}

	source := &KafkaSource{
		client:                      c,
		sink:                        &sink,
		cfg:                         config,
		kafkaRegistry:               go_metrics.NewRegistry(),
		previousTopicOffset:         previousTopicOffset,
		previousConsumerGroupOffset: previousConsumerGroupOffset,
	}

	// Oldest topic p0
	offsetResponse := new(sarama.OffsetResponse)
	offsetResponse.AddTopicPartition("my_topic", 0, 0)
	seedBroker.Returns(offsetResponse)

	// New topic p0
	offsetResponseNewest := new(sarama.OffsetResponse)
	offsetResponseNewest.AddTopicPartition("my_topic", 0, 1)
	seedBroker.Returns(offsetResponseNewest)

	// Oldest topic p1
	offsetResponsep1 := new(sarama.OffsetResponse)
	offsetResponsep1.AddTopicPartition("my_topic", 1, 0)
	seedBroker.Returns(offsetResponsep1)

	// New topic p1
	offsetResponseNewestp1 := new(sarama.OffsetResponse)
	offsetResponseNewestp1.AddTopicPartition("my_topic", 1, 2)
	seedBroker.Returns(offsetResponseNewestp1)

	offsets, err := source.fetchLastOffsetsMetrics(time.Now())

	if len(offsets) == 0 {
		t.Fatal(err)
	}
	if len(offsets["my_topic"]) != 2 {
		t.Fail()
	}
	if offsets["my_topic"][0] != 1 || offsets["my_topic"][1] != 2 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_topic_partition_my_topic").(metrics.Gauge).Snapshot().Value() != 2 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_topic_partition_offset_oldest_my_topic_0").(metrics.Gauge).Snapshot().Value() != 0 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_topic_partition_offset_newest_my_topic_0").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_topic_partition_offset_oldest_my_topic_1").(metrics.Gauge).Snapshot().Value() != 0 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_topic_partition_offset_newest_my_topic_1").(metrics.Gauge).Snapshot().Value() != 2 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_leader_topic_partition_my_topic_0").(metrics.Gauge).Snapshot().Value() != int64(seedBroker.BrokerID()) {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_leader_topic_partition_my_topic_1").(metrics.Gauge).Snapshot().Value() != int64(seedBroker.BrokerID()) {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_replicas_topic_partition_my_topic_0").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_replicas_topic_partition_my_topic_1").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_in_sync_replicas_my_topic_0").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_in_sync_replicas_my_topic_1").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_leader_is_preferred_topic_partition_my_topic_0").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_leader_is_preferred_topic_partition_my_topic_1").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_under_replicated_topic_partition_my_topic_0").(metrics.Gauge).Snapshot().Value() != 0 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_under_replicated_topic_partition_my_topic_1").(metrics.Gauge).Snapshot().Value() != 0 {
		t.Fail()
	}
}

func TestFetchConsumerGroupMetrics(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()

	replicas := []int32{seedBroker.BrokerID()}

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, seedBroker.BrokerID(), replicas, replicas, []int32{}, sarama.ErrNoError)
	metadataResponse.AddTopicPartition("my_topic", 1, seedBroker.BrokerID(), replicas, replicas, []int32{}, sarama.ErrNoError)
	seedBroker.Returns(metadataResponse)

	config := sarama.NewConfig()
	config.Metadata.Retry.Max = 0
	config.Version = sarama.V0_9_0_0
	c, err := sarama.NewClient([]string{seedBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()

	previousTopicOffset := make(map[string]int64)
	previousConsumerGroupOffset := make(map[string]int64)

	sink := fakeSink{
		metrics: make([]interface{}, 0),
	}

	source := &KafkaSource{
		client:                      c,
		sink:                        &sink,
		cfg:                         config,
		kafkaRegistry:               go_metrics.NewRegistry(),
		previousTopicOffset:         previousTopicOffset,
		previousConsumerGroupOffset: previousConsumerGroupOffset,
	}

	listConsumerGroupResponse := &sarama.ListGroupsResponse{
		Groups: map[string]string{
			"my_group": "consumer",
		},
	}
	seedBroker.Returns(listConsumerGroupResponse)

	lastOffsets := map[string]map[int32]int64{
		"my_topic": {
			0: 1,
			1: 2,
		},
	}

	coordianatorResponse := &sarama.FindCoordinatorResponse{
		Coordinator: c.Brokers()[0],
	}
	seedBroker.Returns(coordianatorResponse)

	consumerGroupMemberMetadata := &sarama.ConsumerGroupMemberMetadata{
		Version:  0,
		Topics:   []string{"my_topic"},
		UserData: []byte{},
	}

	reConsumerGroupMemberMetadata := newRealEncoder(20)
	encodeConsumerGroupMemberMetadata(reConsumerGroupMemberMetadata, consumerGroupMemberMetadata)

	consumerGroupMemberAssignement := &sarama.ConsumerGroupMemberAssignment{
		Version: 0,
		Topics: map[string][]int32{
			"my_topic": {
				0,
				1,
			},
		},
		UserData: []byte{},
	}

	reConsumerGroupMemberAssignement := newRealEncoder(32)
	encodeConsumerGroupMemberAssignment(reConsumerGroupMemberAssignement, consumerGroupMemberAssignement)

	describeGroupResponse := &sarama.DescribeGroupsResponse{
		Groups: []*sarama.GroupDescription{
			{
				GroupId:      "test",
				ProtocolType: "consumer",
				State:        "Stable",
				Members: map[string]*sarama.GroupMemberDescription{
					"my_topic": {
						ClientId:         "test",
						ClientHost:       "localhost",
						MemberMetadata:   reConsumerGroupMemberMetadata.raw,
						MemberAssignment: reConsumerGroupMemberAssignement.raw,
					},
				},
			},
		},
	}
	seedBroker.Returns(describeGroupResponse)

	offsetFetchResponse := new(sarama.OffsetFetchResponse)
	offsetFetchResponse.AddBlock("my_topic", 0, &sarama.OffsetFetchResponseBlock{
		Offset: 0,
	})
	offsetFetchResponse.AddBlock("my_topic", 1, &sarama.OffsetFetchResponseBlock{
		Offset: 1,
	})
	seedBroker.Returns(offsetFetchResponse)

	err = source.fetchConsumerGroupMetrics(time.Now(), lastOffsets)
	if err != nil {
		t.Fatal(err)
	}

	if source.kafkaRegistry.Get("kafka_consumer_group_lag_my_group_my_topic_0").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_consumer_group_lag_my_group_my_topic_1").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_consumer_group_latest_offset_my_group_my_topic_0").(metrics.Gauge).Snapshot().Value() != 0 {
		t.Fail()
	}

	if source.kafkaRegistry.Get("kafka_consumer_group_latest_offset_my_group_my_topic_1").(metrics.Gauge).Snapshot().Value() != 1 {
		t.Fail()
	}
}

type fakeSink struct {
	metricsChan chan interface{}
	metrics     []interface{}

	sync.WaitGroup
}

func (s *fakeSink) GetMetricsChan() chan<- interface{} {
	return s.metricsChan
}
func (s *fakeSink) Close() error {
	close(s.metricsChan)
	s.Wait()

	return nil
}
func (s *fakeSink) Run() {
	s.Add(1)
	go func() {
		defer s.Done()
		for m := range s.metricsChan {
			s.metrics = append(s.metrics, m)
		}
	}()
}

func (s *fakeSink) CleanUp() {
	s.metrics = nil
}

func encodeConsumerGroupMemberAssignment(pe realEncoder, m *sarama.ConsumerGroupMemberAssignment) error {
	pe.putInt16(m.Version)

	if err := pe.putArrayLength(len(m.Topics)); err != nil {
		return err
	}

	for topic, partitions := range m.Topics {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putInt32Array(partitions); err != nil {
			return err
		}
	}

	if err := pe.putBytes(m.UserData); err != nil {
		return err
	}

	return nil
}

func encodeConsumerGroupMemberMetadata(pe realEncoder, m *sarama.ConsumerGroupMemberMetadata) error {
	pe.putInt16(m.Version)

	if err := pe.putStringArray(m.Topics); err != nil {
		return err
	}

	if err := pe.putBytes(m.UserData); err != nil {
		return err
	}

	return nil
}

func newRealEncoder(size int64) realEncoder {
	raw := make([]byte, size)
	return realEncoder{
		raw: raw,
		off: 0,
	}
}

type realEncoder struct {
	raw []byte
	off int
}

func (re *realEncoder) putInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *realEncoder) putArrayLength(in int) error {
	re.putInt32(int32(in))
	return nil
}

func (re *realEncoder) putStringArray(in []string) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.putString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *realEncoder) putInt32Array(in []int32) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *realEncoder) putInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *realEncoder) putBytes(in []byte) error {
	if in == nil {
		re.putInt32(-1)
		return nil
	}
	re.putInt32(int32(len(in)))
	return re.putRawBytes(in)
}

func (re *realEncoder) putRawBytes(in []byte) error {
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putString(in string) error {
	re.putInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}
