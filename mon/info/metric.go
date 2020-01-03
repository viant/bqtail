package info

import (
	"fmt"
	"time"
)

//Metric represents a metrc
type Metric struct {
	Key      string `json:",omitempty"`
	Count    int
	Min      *time.Time `json:",omitempty"`
	Max      *time.Time `json:",omitempty"`
	Lag      string     `json:",omitempty"`
	LagInSec int        `json:",omitempty"`
}

//Add adds a metric
func (m *Metric) Add(metric *Metric, addLag bool) {
	if m.Max == nil {
		m.Max = metric.Max
	}
	if metric.Max.After(*m.Max) {
		m.Max = metric.Max
	}

	if m.Min == nil {
		m.Min = metric.Min
	}
	if metric.Min.Before(*m.Min) {
		m.Min = metric.Min
	}

	if addLag {
		lagInSec := time.Now().Sub(*m.Min).Seconds()
		if lagInSec < 0 {
			lagInSec = -1 * lagInSec
		}
		if lagInSec > 0 {
			m.LagInSec = int(lagInSec)
			m.Lag = fmt.Sprintf("%s", time.Second*time.Duration(lagInSec))
		}
	}
	m.Count += metric.Count
}

//AddEvent adds an event
func (m *Metric) AddEvent(ts time.Time) {
	m.Count++
	if m.Min == nil {
		m.Min = &ts
	}
	if ts.Before(*m.Min) {
		m.Min = &ts
	}
	if m.Max == nil {
		m.Max = &ts
	}
	if ts.After(*m.Max) {
		m.Max = &ts
	}
}

//NewMetric creates a metrc
func NewMetric() *Metric {
	return &Metric{}
}
