package tail

import (
	"bqtail/base"
	"bqtail/tail/batch"
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
	Window        *batch.Window `json:",ommittempty"`
}

func (j Job) Dest() string {
	if j.Load == nil {
		return ""
	}
	return j.Load.DestinationTable.DatasetId + base.PathElementSeparator + j.Load.DestinationTable.TableId
}
