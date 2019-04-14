package common

import (
	"sync"

	"github.com/ryarnyah/kafka-offset/pkg/metrics"
	"github.com/sirupsen/logrus"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	MetricsChan    chan interface{}
	KafkaMeterFunc func(metrics.KafkaMeter) error
	KafkaGaugeFunc func(metrics.KafkaGauge) error

	CloseFunc func() error

	wg sync.WaitGroup
}

// GetMetricsChan metrics chan
func (s *Sink) GetMetricsChan() chan<- interface{} {
	return s.MetricsChan
}

// Close do nothing
func (s *Sink) Close() error {

	close(s.MetricsChan)
	s.wg.Wait()
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
	s.MetricsChan = make(chan interface{}, 1024)

	return s
}

// Run start consume all channels
func (s *Sink) Run() {
	s.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for metric := range s.MetricsChan {
			switch metric := metric.(type) {
			case metrics.KafkaMeter:
				err := s.KafkaMeterFunc(metric)
				if err != nil {
					logrus.Error(err)
				}
			case metrics.KafkaGauge:
				err := s.KafkaGaugeFunc(metric)
				if err != nil {
					logrus.Error(err)
				}
			}
		}
	}(s)
}
