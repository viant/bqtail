package contract

import "bqtail/stage"

type Metrics struct {
	CopyJobs  int `json:",omitempty"`
	QueryJobs int `json:",omitempty"`
	LoadJobs  int `json:",omitempty"`
	OtherJobs int `json:",omitempty"`
}

//Update updates a metrics with job ID
func (m *Metrics) Update(jobID string) *stage.Info {
	stageInfo := stage.Parse(jobID)
	m.Add(stageInfo, 1)
	return stageInfo
}

//Update updates a metrics with job ID
func (m *Metrics) Add(stageInfo *stage.Info, count int) {
	switch stageInfo.Action {
	case "query":
		m.QueryJobs+=count
	case "copy":
		m.CopyJobs+=count
	case "load", "reload":
		m.LoadJobs+=count
	default:
		m.OtherJobs+=count
	}
}

func (m *Metrics) Merge(metrics *Metrics) {
	m.CopyJobs+=metrics.CopyJobs
	m.QueryJobs+=metrics.QueryJobs
	m.LoadJobs+=metrics.LoadJobs
	m.OtherJobs+=metrics.OtherJobs
}
