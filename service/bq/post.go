package bq

import (
	"bqtail/base"
	"bqtail/stage"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)

func (s *service) setJobID(ctx context.Context, actions *task.Actions) (*bigquery.JobReference, error) {
	var ID string
	if actions != nil {
		ID = actions.Info.GetJobID()
	}
	return &bigquery.JobReference{
		JobId:     ID,
		ProjectId: s.Config.ProjectID,
	}, nil
}

func (s *service) schedulePostTask(ctx context.Context, job *bigquery.Job, actions *task.Actions) error {
	if actions == nil || actions.IsEmpty() || actions.IsSyncMode() {
		return nil
	}
	actions.Job = job
	data, err := json.Marshal(actions)
	if err != nil {
		return errors.Wrapf(err, "failed to encode actions: %v", actions)
	}
	filename := actions.Info.JobFilename()
	URL := url.Join(s.Config.AsyncTaskURL, filename)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

//Post post big query job
func (s *service) Post(ctx context.Context, projectID string, callerJob *bigquery.Job, onDoneActions *task.Actions) (*bigquery.Job, error) {
	if onDoneActions != nil {
		if onDoneActions.JobID != "" && onDoneActions.Info.Action == "" {
			onDoneActions.Info = *stage.Parse(onDoneActions.JobID)
			//Legacy transition
			onDoneActions.Info.Step = strings.Count(onDoneActions.JobID, stage.PathElementSeparator)
		}
	}
	job, err := s.post(ctx, projectID, callerJob, onDoneActions)
	if job == nil {
		job = callerJob
	} else {
		callerJob.Id = job.Id
	}
	if base.IsLoggingEnabled() {
		fmt.Printf("Job status: %v %v\n", callerJob.Id, err)
		toolbox.Dump(job)
	}

	if onDoneActions != nil && onDoneActions.IsSyncMode() {
		err = base.JobError(job)
		if err == nil {
			job, err = s.Wait(ctx, job.JobReference)
			if err == nil {
				err = base.JobError(job)
			}
		}
		if job == nil {
			job = callerJob
		}
		if e := s.runActions(ctx, err, job, onDoneActions); e != nil {
			if err == nil {
				err = e
			} else {
				err = errors.Wrapf(err, "failed to run post action: %v", e)
			}
		}
	}
	return job, err
}

func (s *service) post(ctx context.Context, projectID string, job *bigquery.Job, onDoneActions *task.Actions) (*bigquery.Job, error) {
	var err error
	if job.JobReference, err = s.setJobID(ctx, onDoneActions); err != nil {
		return nil, err
	}
	err = s.schedulePostTask(ctx, job, onDoneActions)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to schedule bqJob %v", job.JobReference.JobId)
	}
	if base.IsLoggingEnabled() {
		toolbox.Dump(job)
		fmt.Printf("OnDone: ")
		toolbox.Dump(onDoneActions)
	}

	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Insert(projectID, job)
	call.Context(ctx)
	var callJob *bigquery.Job
	for i := 0; i < base.MaxRetries; i++ {
		if callJob, err = call.Do(); err == nil {
			break
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(base.RetrySleepInSec * time.Second)
			continue
		}
		if i > 0 && base.IsDuplicateJobError(err) {
			if base.IsLoggingEnabled() {
				fmt.Printf("duplicate job: [%v]: %v\n", job.Id, err)
			}
			err = nil
		}
	}
	if err != nil || (callJob != nil && base.JobError(callJob) != nil) {
		return callJob, err
	}
	return s.GetJob(ctx, projectID, job.JobReference.JobId)
}
