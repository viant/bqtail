package tail

import (
	"bqtail/base"
	"bqtail/stage"
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
	Window    *batch.Window `json:",ommittempty"`
	Rule      *config.Rule
	DestTable string
}

//GetSourceURI returns data source URI
func (j Job) GetSourceURI() string {
	if j.Window != nil && j.Window.SourceURL != "" {
		return j.Window.SourceURL
	}
	if j.Actions != nil && j.Actions.SourceURI != "" {
		return j.Actions.SourceURI
	}
	if j.Load != nil && len(j.Load.SourceUris) > 0 {
		return j.Load.SourceUris[0]
	}
	return ""
}

//IDSuffix returns jobID suffix
func (j Job) IDSuffix() string {
	suffix := stage.DispatchJob
	if j.IsSyncMode() {
		suffix = stage.TailJob
	}
	return suffix
}

//GetJobID returns job ID
func (j Job) Info() *stage.Info {
	dest := j.Dest()
	if ref, err := base.NewTableReference(dest); err == nil {
		dest = ref.DatasetId + "." + ref.TableId
	}
	source := ""
	ruleURL := ""
	async := false
	if j.Rule != nil {
		async = j.Rule.Async
		ruleURL = j.Rule.Info.URL
	}
	if j.Window != nil {
		source = j.Window.SourceURL
	}
	return stage.New(source, dest, j.EventID, "load", j.IDSuffix(), async, 0, ruleURL)

}

//IsSyncMode returns true if in sync mode
func (j Job) IsSyncMode() bool {
	return !j.Rule.Async
}

//Table returns dataset and table destination
func (j Job) Dest() string {
	if j.Load == nil {
		return ""
	}
	if j.DestTable != "" {
		return j.DestTable
	}
	return j.Load.DestinationTable.DatasetId + stage.PathElementSeparator + j.Load.DestinationTable.TableId
}

//SetIfError sets non nil error
func (j *Job) SetIfError(err error) {
	if err == nil {
		return
	}
	j.Status = base.StatusError
	j.Error = err.Error()
}
