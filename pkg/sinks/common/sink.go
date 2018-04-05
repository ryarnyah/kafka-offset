package common

import (
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/ryarnyah/kafka-offset/pkg/metrics"
)

// Sink default sink use logrus to print metrics
type Sink struct {
	MetricsChan    chan interface{}
	KafkaMeterFunc func(metrics.KafkaMeter) error
	KafkaGaugeFunc func(metrics.KafkaGauge) error

	CloseFunc func() error

	StopCh chan interface{}

	wg sync.WaitGroup
}

// GetMetricsChan metrics chan
func (s *Sink) GetMetricsChan() chan<- interface{} {
	return s.MetricsChan
}

// Close do nothing
func (s *Sink) Close() error {
	close(s.StopCh)
	s.wg.Wait()
	close(s.MetricsChan)
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
	s.MetricsChan = make(chan interface{}, 1024)

	return s
}

// Run start consume all channels
func (s *Sink) Run() {
	s.wg.Add(1)
	go func(s *Sink) {
		defer s.wg.Done()
		for {
			select {
			case metric := <-s.MetricsChan:
				switch metric.(type) {
				case metrics.KafkaMeter:
					err := s.KafkaMeterFunc(metric.(metrics.KafkaMeter))
					if err != nil {
						logrus.Error(err)
					}
				case metrics.KafkaGauge:
					err := s.KafkaGaugeFunc(metric.(metrics.KafkaGauge))
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
