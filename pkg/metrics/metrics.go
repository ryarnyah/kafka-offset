package metrics

import (
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

// BaseMetric base metrics for gauge and meter
type BaseMetric struct {
	Name      string
	Key       string
	Timestamp time.Time
	Meta      map[string]any
}

// KafkaMeter meter with metadata
type KafkaMeter struct {
	BaseMetric
	metrics.Meter
}

// Snapshot get meter snapshot and set Timestamp
func (m KafkaMeter) Snapshot() metrics.Meter {
	return KafkaMeter{
		BaseMetric: BaseMetric{
			Name:      m.Name,
			Timestamp: time.Now(),
			Meta:      m.Meta,
			Key:       m.Key,
		},
		Meter: m.Meter.Snapshot(),
	}
}

// NewKafkaMeter build a new Kafka Meter
func NewKafkaMeter(name, key string, meta map[string]any) KafkaMeter {
	return KafkaMeter{
		BaseMetric: BaseMetric{
			Name: name,
			Meta: meta,
			Key:  key,
		},
		Meter: metrics.NewMeter(),
	}
}

// KafkaGauge gauge with metadata
type KafkaGauge struct {
	BaseMetric
	metrics.Gauge
}

// Snapshot get gauge snapshot and set Timestamp
func (m KafkaGauge) Snapshot() metrics.Gauge {
	return KafkaGauge{
		BaseMetric: BaseMetric{
			Name:      m.Name,
			Timestamp: time.Now(),
			Meta:      m.Meta,
			Key:       m.Key,
		},
		Gauge: m.Gauge.Snapshot(),
	}
}

// NewKafkaGauge build new kafka gauge
func NewKafkaGauge(name, key string, meta map[string]any) KafkaGauge {
	return KafkaGauge{
		BaseMetric: BaseMetric{
			Name: name,
			Meta: meta,
			Key:  key,
		},
		Gauge: metrics.NewGauge(),
	}
}
