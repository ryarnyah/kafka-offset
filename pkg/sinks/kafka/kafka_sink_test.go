package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"

	common_metrics "github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/ryarnyah/kafka-offset/pkg/sinks/common"
)

type fakeKafkaProducer struct {
	messages []*sarama.ProducerMessage
}

func (s *fakeKafkaProducer) Close() error {
	return nil
}
func (s *fakeKafkaProducer) SendMessage(m *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	s.messages = append(s.messages, m)
	return 0, 0, nil
}

func (s *fakeKafkaProducer) SendMessages(m []*sarama.ProducerMessage) (err error) {
	s.messages = append(s.messages, m...)
	return nil
}

func (s *fakeKafkaProducer) CleanUp() {
	s.messages = nil
}
func TestSendToSink(t *testing.T) {
	producer := fakeKafkaProducer{}
	defer producer.CleanUp()
	sink := &Sink{
		producer: &producer,
		topic:    *kafkaSinkTopic,
		Sink:     common.NewCommonSink(),
	}
	sink.KafkaMeterFunc = sink.kafkaMeter
	sink.KafkaGaugeFunc = sink.kafkaGauge
	sink.CloseFunc = sink.closeProducer

	sink.Run()

	sink.GetMetricsChan() <- common_metrics.KafkaMeter{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Meter: metrics.NilMeter{},
	}

	sink.GetMetricsChan() <- common_metrics.KafkaGauge{
		BaseMetric: common_metrics.BaseMetric{
			Name:      "toto",
			Key:       "titi",
			Timestamp: time.Now(),
			Meta:      make(map[string]interface{}),
		},
		Gauge: metrics.NilGauge{},
	}

	sink.Close()

	if len(producer.messages) != 2 {
		t.Fail()
	}
}
