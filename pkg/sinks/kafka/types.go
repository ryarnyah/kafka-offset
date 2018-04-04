package kafka

import "time"

// OffsetMetric metric for topic/partition with oldest and newest offset
type OffsetMetric struct {
	Name         string    `json:"name,omitempty"`
	Timestamp    time.Time `json:"timestamp,omitempty"`
	Topic        string    `json:"topic,omitempty"`
	Partition    int32     `json:"partition,omitempty"`
	OldestOffset int64     `json:"oldest_offset,omitempty"`
	NewestOffset int64     `json:"newest_offset,omitempty"`
}

// ConsumerGroupOffsetMetric metric for consumer group
type ConsumerGroupOffsetMetric struct {
	Name      string    `json:"name,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Group     string    `json:"group,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Partition int32     `json:"partition,omitempty"`
	Offset    int64     `json:"offset,omitempty"`
	Lag       int64     `json:"lag,omitempty"`
}

// TopicRateMetric rate topic writes per seconds
type TopicRateMetric struct {
	Name      string    `json:"name,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Rate1     float64   `json:"rate1,omitempty"`
	Rate5     float64   `json:"rate5,omitempty"`
	Rate15    float64   `json:"rate15,omitempty"`
	RateMean  float64   `json:"rate_mean,omitempty"`
	Count     int64     `json:"count,omitempty"`
}

// ConsumerGroupRateMetric rate consumer group read/commit per seconds
type ConsumerGroupRateMetric struct {
	Name      string    `json:"name,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Group     string    `json:"group,omitempty"`
	Topic     string    `json:"topic,omitempty"`
	Rate1     float64   `json:"rate1,omitempty"`
	Rate5     float64   `json:"rate5,omitempty"`
	Rate15    float64   `json:"rate15,omitempty"`
	RateMean  float64   `json:"rate_mean,omitempty"`
	Count     int64     `json:"count,omitempty"`
}
