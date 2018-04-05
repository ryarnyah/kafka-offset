package elasticsearch

import "time"

type meter struct {
	Name      string                 `json:"name,omitempty"`
	Timestamp time.Time              `json:"timestamp,omitempty"`
	Meta      map[string]interface{} `json:"meta,omitempty"`
	Rate1     float64                `json:"rate1,omitempty"`
	Rate5     float64                `json:"rate5,omitempty"`
	Rate15    float64                `json:"rate15,omitempty"`
	RateMean  float64                `json:"rate_mean,omitempty"`
	Count     int64                  `json:"count,omitempty"`
}

type gauge struct {
	Name      string                 `json:"name,omitempty"`
	Timestamp time.Time              `json:"timestamp,omitempty"`
	Meta      map[string]interface{} `json:"meta,omitempty"`
	Value     int64                  `json:"value,omitempty"`
}
