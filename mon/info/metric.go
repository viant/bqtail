package info

import (
	"fmt"
	"time"
)

//Metric represents a metrc
type Metric struct {
	Count      int       `json:",omitempty"`
	Min        time.Time `json:",omitempty"`
	Max        time.Time `json:",omitempty"`
	Delay      string    `json:",omitempty"`
	DelayInSec int       `json:",omitempty"`
}

//Add adds a metric
func (m *Metric) Add(metric *Metric, addDelay bool) {
	if m.Max.IsZero() {
		m.Max = metric.Max
	}
	if metric.Min.Before(m.Min) {
		m.Min = metric.Min
	}
	if m.Min.IsZero() {
		m.Min = metric.Min
	}
	if metric.Max.After(m.Max) {
		m.Max = metric.Max
	}
	if addDelay {
		delayInSec := time.Now().Sub(m.Min).Seconds()
		if delayInSec > 0 {
			m.DelayInSec = int(delayInSec)
			m.Delay = fmt.Sprintf("%s", (time.Second * time.Duration(delayInSec)))
		}
	}
	m.Count += metric.Count
}

//AddEvent adds an event
func (m *Metric) AddEvent(ts time.Time) {
	m.Count++
	if m.Min.IsZero() {
		m.Min = ts
	}
	if ts.Before(m.Min) {
		m.Min = ts
	}
	if m.Max.IsZero() {
		m.Max = ts
	}
	if ts.After(m.Max) {
		m.Max = ts
	}
}

//NewMetric creates a metrc
func NewMetric() *Metric {
	return &Metric{}
}
