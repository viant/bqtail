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
	EventID       string
	SourceCreated time.Time
	Job           *bigquery.Job
	Load          *bigquery.JobConfigurationLoad
	Error         string `json:",ommittempty"`
	Status        string
	*task.Actions
	Window        *batch.Window `json:",ommittempty"`
}

//Dest returns dataset and table destination
func (j Job) Dest() string {
	if j.Load == nil {
		return ""
	}
	return j.Load.DestinationTable.DatasetId + base.PathElementSeparator + j.Load.DestinationTable.TableId
}

//SetIfError sets non nil error
func (r *Job) SetIfError(err error) {
	if err == nil {
		return
	}
	r.Status = base.StatusError
	r.Error = err.Error()
}
