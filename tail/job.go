package tail

import (
	"bqtail/base"
	"bqtail/tail/batch"
	"bqtail/tail/config"
	"bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Job represents a tail jobID
type Job struct {
	EventID       string                         `json:"eventID,ommittempty"`
	SourceCreated time.Time                      `json:"created,ommittempty"`
	Statistics    *bigquery.JobStatistics        `json:"statistics,omitempty"`
	JobStatus     *bigquery.JobStatus            `json:"jobStatus,omitempty"`
	Load          *bigquery.JobConfigurationLoad `json:"load,ommittempty"`
	Error         string                         `json:",ommittempty"`
	Status        string                         `json:",ommittempty"`
	*task.Actions
	Window *batch.Window `json:",ommittempty"`
	Rule   *config.Rule
}

//IDSuffix returns jobID suffix
func (j Job) IDSuffix() string {
	suffix := base.DispatchJob
	if j.IsSyncMode() {
		suffix = base.TailJob
	}
	return suffix
}

//IsSyncMode returns true if in sync mode
func (j Job) IsSyncMode() bool {
	return !j.Rule.Async
}

//Dest returns dataset and table destination
func (j Job) Dest() string {
	if j.Load == nil {
		return ""
	}
	return j.Load.DestinationTable.DatasetId + base.PathElementSeparator + j.Load.DestinationTable.TableId
}

//SetIfError sets non nil error
func (j *Job) SetIfError(err error) {
	if err == nil {
		return
	}
	j.Status = base.StatusError
	j.Error = err.Error()
}
