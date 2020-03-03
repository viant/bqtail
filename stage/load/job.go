package load

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

//Job represents a tail jobID
type Job struct {
	*stage.Process `json:",omitempty"`
	Rule           *config.Rule                   `json:"-"`
	Status         string                         `json:",omitempty"`
	Window         *batch.Window                  `json:",omitempty"`
	Statistics     *bigquery.JobStatistics        `json:"statistics,omitempty"`
	JobStatus      *bigquery.JobStatus            `json:"jobStatus,omitempty"`
	Load           *bigquery.JobConfigurationLoad `json:"load,ommittempty"`
	TempSchema     *bigquery.Table                `json:",omitempty"`
	SplitSchema    *bigquery.Table                `json:",omitempty"`
	DestSchema     *bigquery.Table                `json:",omitempty"`
	Actions        *task.Actions                  `json:",omitempty"`
	BqJob          *bigquery.Job                  `json:"-"`
	splitColumns   []*bigquery.TableFieldSchema
}

//Recoverable returns true if recoverable
func (j *Job) Recoverable() bool {
	if j == nil {
		return false
	}
	if j.BqJob == nil {
		return true
	}
	return j.BqJob.Status != nil && j.BqJob.Status.ErrorResult != nil
}

//Persist persist a job
func (j *Job) Persist(ctx context.Context, fs afs.Service) error {
	JSON, err := json.Marshal(j)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal job %+v", j)
	}
	err = fs.Upload(ctx, j.ProcessURL, file.DefaultFileOsMode, bytes.NewReader(JSON))
	return err
}

//IsSyncMode returns true if in sync mode
func (j Job) IsSyncMode() bool {
	return !j.Process.Async
}

//Dest returns dataset and table destination
func (j Job) Dest() string {
	if j.Load == nil {
		return ""
	}
	if j.DestTable != "" {
		return j.DestTable
	}
	return j.Load.DestinationTable.DatasetId + shared.PathElementSeparator + j.Load.DestinationTable.TableId
}

//NewJob create a load job
func NewJob(rule *config.Rule, process *stage.Process, window *batch.Window) (*Job, error) {
	job := &Job{
		Status:  shared.StatusOK,
		Rule:    rule,
		Process: process,
		Window:  window,
	}
	var err error
	if window != nil {
		job.Load, err = rule.Dest.NewJobConfigurationLoad(process.Source, window.URIs...)
	} else {
		job.Load, err = rule.Dest.NewJobConfigurationLoad(process.Source, process.Source.URL)
	}
	return job, err
}

//NewJobFromURL create a job from url
func NewJobFromURL(ctx context.Context, rule *config.Rule, processURL string, fs afs.Service) (*Job, error) {
	reader, err := fs.DownloadWithURL(ctx, processURL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	job := &Job{}
	err = json.NewDecoder(reader).Decode(job)
	if err != nil {
		return nil, err
	}
	job.Rule = rule
	return job, nil
}
