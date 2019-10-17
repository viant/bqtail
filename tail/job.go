package tail

import (
	"bqtail/base"
	"bqtail/tail/batch"
	"bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Job represents a tail job
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
