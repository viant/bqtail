package bq

import (
	"bqtail/base"
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
	"path"
	"time"
)

var syncCheckTimeout = 2 * time.Second

func (s *service) setJobID(ctx context.Context, actions *task.Actions) (*bigquery.JobReference, error) {
	var ID string
	var err error
	if actions != nil {
		if ID, err = actions.ID(base.JobPrefix); err != nil {
			return nil, errors.Wrapf(err, "failed to generate job ID: %v", actions.JobID)
		}
	}
	return &bigquery.JobReference{
		JobId: ID,
		ProjectId:s.Config.ProjectID,
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
	filename := base.DecodePathSeparator(job.JobReference.JobId, 1)
	if path.Ext(filename) == "" {
		filename += base.JobExt
	}
	URL := url.Join(actions.DeferTaskURL, filename)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

//Post post big query job
func (s *service) Post(ctx context.Context, projectID string, callerJob *bigquery.Job, onDoneActions *task.Actions) (*bigquery.Job, error) {
	job, err := s.post(ctx, projectID, callerJob, onDoneActions)
	if err == nil {
		err = base.JobError(job)
	}
	if job == nil {
		job = callerJob
	} else {
		callerJob.Id = job.Id
	}




	if onDoneActions != nil && (onDoneActions.IsSyncMode() || err != nil) {
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
	} else {
		job, err = s.waitWithTimeout(ctx, job.JobReference, syncCheckTimeout)
		if base.IsLoggingEnabled() {
			fmt.Printf("Job status: %v %v\n", callerJob.Id, err)
			toolbox.Dump(job)
		}
	}
	return job, err
}

func (s *service) post(ctx context.Context, projectID string, job *bigquery.Job, onDoneActions *task.Actions) (*bigquery.Job, error) {
	var err error
	if job.JobReference, err = s.setJobID(ctx, onDoneActions); err != nil {
		return nil, err
	}
	if job.JobReference != nil {
		job.JobReference.JobId = base.EncodePathSeparator(job.JobReference.JobId)
	}
	if err = s.schedulePostTask(ctx, job, onDoneActions); err != nil {
		return nil, err
	}
	if base.IsLoggingEnabled() {
		toolbox.Dump(job)
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
			err = nil
		}
	}
	if err != nil || (callJob != nil && base.JobError(callJob) != nil) {
		return callJob, err
	}
	return s.GetJob(ctx, projectID, job.JobReference.JobId)
}
