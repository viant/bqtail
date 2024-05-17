package contract

import (
	"fmt"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage/activity"
	"strings"
	"sync/atomic"
)

// ProjectPerformance represents project performance
type ProjectPerformance map[string]*Performance

// Performance performance
type Performance struct {
	ProjectID  string   `json:",omitempty"`
	Region     string   `json:",omitempty"`
	Count      uint32   `json:",omitempty"`
	Running    *Metrics `json:",omitempty"`
	Pending    *Metrics `json:",omitempty"`
	Dispatched *Metrics `json:",omitempty"`
	Throttled  *Metrics `json:",omitempty"`
	NoFound    int      `json:",omitempty"`
}

// Merge merges performance
func (p *Performance) Merge(perf *Performance) {
	if perf.Running.Count() > 0 {
		if p.Running == nil {
			p.Running = perf.Running
		} else {
			p.Running.QueryJobs += perf.Running.QueryJobs
			p.Running.LoadJobs += perf.Running.LoadJobs
			p.Running.CopyJobs += perf.Running.CopyJobs
			p.Running.BatchJobs += perf.Running.BatchJobs
			p.Running.OtherJobs += perf.Running.OtherJobs
		}
	}
	if perf.Pending.Count() > 0 {
		if p.Pending == nil {
			p.Pending = perf.Pending
		} else {
			p.Pending.QueryJobs += perf.Pending.QueryJobs
			p.Pending.LoadJobs += perf.Pending.LoadJobs
			p.Pending.CopyJobs += perf.Pending.CopyJobs
		}
	}
	p.NoFound += perf.NoFound
	p.Count += perf.Count
	p.Dispatched.Merge(perf.Dispatched)
	p.Throttled.Merge(perf.Throttled)
}

// ActiveQueryCount returns active query count
func (p *Performance) ActiveQueryCount() int {
	return p.Pending.QueryJobs +
		p.Running.QueryJobs
}

// ActiveLoadCount returns active query count
func (p *Performance) ActiveLoadCount() int {
	result := 0
	if p.Pending != nil {
		result += p.Pending.LoadJobs
	}
	if p.Running != nil {
		result += p.Running.LoadJobs
	}
	return result
}

// AddEvent adds running, pending metrics
func (p *Performance) AddEvent(state string, jobID string) {
	atomic.AddUint32(&p.Count, 1)
	metrics := p.Metric(state)
	if metrics != nil {
		metrics.Update(jobID)
	}
}

// Metric returns a metric
func (p *Performance) Metric(state string) *Metrics {
	var metrics *Metrics
	switch strings.ToUpper(state) {
	case shared.RunningState:
		metrics = p.Running
	case shared.PendingState:
		metrics = p.Pending
	}
	return metrics
}

// AddDispatch add dispatched metrics
func (p *Performance) AddDispatch(jobID string) *activity.Meta {
	return p.Dispatched.Update(jobID)
}

// AddThrottled add throttled metrics
func (p *Performance) AddThrottled(jobID string) {
	stageInfo := p.Throttled.Update(jobID)
	p.Dispatched.Add(stageInfo, -1)
}

// String return performance string
func (p *Performance) String() string {
	return fmt.Sprintf("%v: events: %v, dipatched: {batched: %v, load: %v, copy:%v, query: %v}, pending: {load:%v, copy: %v,  query: %v}, running: {load : %v, copy: %v, query: %v}, noFound: %v\n", p.ProjectID, p.Count, p.Dispatched.BatchJobs, p.Dispatched.LoadJobs, p.Dispatched.CopyJobs, p.Dispatched.QueryJobs, p.Pending.LoadJobs, p.Pending.CopyJobs, p.Pending.QueryJobs, p.Running.LoadJobs, p.Running.CopyJobs, p.Running.QueryJobs, p.NoFound)
}

// NewPerformance create a performance
func NewPerformance() *Performance {
	return &Performance{
		Running:    &Metrics{},
		Pending:    &Metrics{},
		Dispatched: &Metrics{},
		Throttled:  &Metrics{},
	}
}
