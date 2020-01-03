package contract

import "bqtail/stage"

//Metrics represents BqQuery jobs metric
type Metrics struct {
	CopyJobs     int `json:",omitempty"`
	QueryJobs    int `json:",omitempty"`
	LoadProcesss int `json:",omitempty"`
	OtherJobs    int `json:",omitempty"`
}

//Count returns total metrics count
func (m Metrics) Count() int {
	return m.QueryJobs + m.CopyJobs + m.LoadProcesss + m.OtherJobs
}

//Update updates a metrics with job ID
func (m *Metrics) Update(jobID string) *stage.Info {
	stageInfo := stage.Parse(jobID)
	m.Add(stageInfo, 1)
	return stageInfo
}

//Add updates a metrics with supplied stage action and count
func (m *Metrics) Add(stageInfo *stage.Info, count int) {
	switch stageInfo.Action {
	case "query":
		m.QueryJobs += count
	case "copy":
		m.CopyJobs += count
	case "load", "reload":
		m.LoadProcesss += count
	default:
		m.OtherJobs += count
	}
}

//Merge merges metrics
func (m *Metrics) Merge(metrics *Metrics) {
	m.CopyJobs += metrics.CopyJobs
	m.QueryJobs += metrics.QueryJobs
	m.LoadProcesss += metrics.LoadProcesss
	m.OtherJobs += metrics.OtherJobs
}
