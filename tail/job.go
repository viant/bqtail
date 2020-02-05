package tail

import (
	"bqtail/base"
	"bqtail/shared"
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
	TempSchema    *bigquery.TableSchema
	DestSchema    *bigquery.TableSchema
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

//Info returns job info
func (j Job) Info() *stage.Info {
	dest := j.Dest()
	if ref, err := base.NewTableReference(dest); err == nil {
		dest = ref.DatasetId + "." + ref.TableId
		if ref.ProjectId != "" {
			dest = ref.ProjectId + ":" + dest
		}
	}
	source := ""
	ruleURL := ""
	async := false
	if j.Rule != nil {
		async = j.Rule.Async
		ruleURL = j.Rule.Info.URL
	}
	projectID := ""
	if j.Window != nil {
		source = j.Window.SourceURL
		projectID = j.Window.ProjectID
	}
	if projectID == "" {
		if j.Load != nil && j.Load.DestinationTable != nil {
			projectID = j.Load.DestinationTable.ProjectId
		}
	}
	return stage.New(projectID, source, dest, j.EventID, "load", j.IDSuffix(), async, 0, ruleURL)
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
	j.Status = shared.StatusError
	j.Error = err.Error()
}
