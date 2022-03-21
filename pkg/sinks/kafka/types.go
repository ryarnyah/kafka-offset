package kafka

import "time"

type meter struct {
	Name      string         `json:"name"`
	Timestamp time.Time      `json:"timestamp"`
	Meta      map[string]any `json:"meta,omitempty"`
	Rate1     float64        `json:"rate1"`
	Rate5     float64        `json:"rate5"`
	Rate15    float64        `json:"rate15"`
	RateMean  float64        `json:"rate_mean"`
	Count     int64          `json:"count"`
}

type gauge struct {
	Name      string         `json:"name"`
	Timestamp time.Time      `json:"timestamp"`
	Meta      map[string]any `json:"meta,omitempty"`
	Value     int64          `json:"value"`
}
