package contract

import (
	"bqtail/base"
	"bqtail/stage"
	"fmt"
	"strings"
	"sync/atomic"
)

//ProjectPerformance represents project performance
type ProjectPerformance map[string]*Performance

//Performance performance
type Performance struct {
	ProjectID  string   `json:",omitempty"`
	Count      uint32   `json:",omitempty"`
	Running    *Metrics `json:",omitempty"`
	Pending    *Metrics `json:",omitempty"`
	Dispatched *Metrics `json:",omitempty"`
	Throttled  *Metrics `json:",omitempty"`
	NoFound    int      `json:",omitempty"`
}

//Merge merges performance
func (p *Performance) Merge(perf *Performance) {
	if perf.Running.Count() > 0 {
		p.Running = perf.Running
	}
	if perf.Pending.Count() > 0 {
		p.Pending = perf.Pending
	}
	p.NoFound += perf.NoFound
	p.Count += perf.Count
	p.Dispatched.Merge(perf.Dispatched)
	p.Throttled.Merge(perf.Throttled)
}

//ActiveQueryCount returns active query count
func (p Performance) ActiveQueryCount() int {
	return p.Pending.QueryJobs +
		p.Running.QueryJobs
}

//ActiveQueryCount returns active query count
func (p Performance) ActiveLoadCount() int {
	return p.Pending.LoadJobs +
		p.Running.LoadJobs
}

//AddEvent adds running, pending metrics
func (p *Performance) AddEvent(state string, jobID string) {
	atomic.AddUint32(&p.Count, 1)
	metrics := p.Metric(state)
	if metrics != nil {
		metrics.Update(jobID)
	}
}

//Metric returns a metric
func (p *Performance) Metric(state string) *Metrics {
	var metrics *Metrics
	switch strings.ToUpper(state) {
	case base.RunningState:
		metrics = p.Running
	case base.PendingState:
		metrics = p.Pending
	}
	return metrics
}

//AddDispatch add dispatched metrics
func (p *Performance) AddDispatch(jobID string) *stage.Info {
	return p.Dispatched.Update(jobID)
}

//AddThrottled add throttled metrics
func (p *Performance) AddThrottled(jobID string) {
	stageInfo := p.Throttled.Update(jobID)
	p.Dispatched.Add(stageInfo, -1)
}

//String return performance string
func (p *Performance) String() string {
	return fmt.Sprintf("%v: events: %v, dipatched: {batched: %v, load: %v, copy:%v, query: %v}, pending: {load:%v, copy: %v,  query: %v}, running: {load : %v, copy: %v, query: %v}, noFound: %v\n", p.ProjectID, p.Count, p.Dispatched.BatchJobs, p.Dispatched.LoadJobs, p.Dispatched.CopyJobs, p.Dispatched.QueryJobs, p.Pending.LoadJobs, p.Pending.CopyJobs, p.Pending.QueryJobs, p.Running.LoadJobs, p.Running.CopyJobs, p.Running.QueryJobs, p.NoFound)
}

//NewPerformance create a performance
func NewPerformance() *Performance {
	return &Performance{
		Running:    &Metrics{},
		Pending:    &Metrics{},
		Dispatched: &Metrics{},
		Throttled:  &Metrics{},
	}
}
