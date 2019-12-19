package contract

import (
	"bqtail/base"
	"bqtail/stage"
	"strings"
)

//Performance performance
type Performance struct {
	Running       *Metrics `json:",omitempty"`
	Pending       *Metrics `json:",omitempty"`
	Dispatched    *Metrics `json:",omitempty"`
	Throttled     *Metrics `json:",omitempty"`
	MissingStatus int      `json:",omitempty"`
}

//Merge merges performance
func (p *Performance) Merge(perf *Performance) {
	p.MissingStatus = perf.MissingStatus
	if perf.Running.Count() > 0 {
		p.Running = perf.Running
	}
	if perf.Pending.Count() > 0 {
		p.Pending = perf.Pending
	}
	p.Dispatched.Merge(perf.Dispatched)
	p.Throttled.Merge(perf.Throttled)
}

//ActiveJobCount returns active jobs (load, copy)
func (p Performance) ActiveJobCount() int {
	return p.Dispatched.LoadJobs + p.Dispatched.CopyJobs +
		p.Pending.LoadJobs + p.Pending.CopyJobs +
		p.Running.LoadJobs + p.Running.CopyJobs
}

//ActiveQueryCount returns active query count
func (p Performance) ActiveQueryCount() int {
	return p.Dispatched.QueryJobs + p.Dispatched.QueryJobs +
		p.Pending.QueryJobs + p.Pending.QueryJobs +
		p.Running.QueryJobs + p.Running.QueryJobs
}

//AddEvent adds running, pending metrics
func (p *Performance) AddEvent(state string, jobID string) {
	var metrics *Metrics
	switch strings.ToUpper(state) {
	case base.RunningState:
		metrics = p.Running
	case base.PendingState:
		metrics = p.Pending
	}
	if metrics != nil {
		metrics.Update(jobID)
	}
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

//NewPerformance create a performance
func NewPerformance() *Performance {
	return &Performance{
		Running:    &Metrics{},
		Pending:    &Metrics{},
		Dispatched: &Metrics{},
		Throttled:  &Metrics{},
	}
}
