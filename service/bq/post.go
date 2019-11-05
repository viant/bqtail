package bq

import (
	"bqtail/base"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"path"
)

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
	}, nil
}

func (s *service) schedulePostTask(ctx context.Context, jobReference *bigquery.JobReference, actions *task.Actions) error {
	if actions == nil || actions.IsEmpty() || actions.IsSyncMode() {
		return nil
	}
	data, err := json.Marshal(actions)
	if err != nil {
		return errors.Wrapf(err, "failed to encode actions: %v", actions)
	}
	filename := base.DecodePathSeparator(jobReference.JobId)
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
	}

	if onDoneActions != nil && onDoneActions.IsSyncMode() {
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
	if job.JobReference != nil {
		job.JobReference.JobId = base.EncodePathSeparator(job.JobReference.JobId)
	}

	if err = s.schedulePostTask(ctx, job.JobReference, onDoneActions); err != nil {
		return nil, err
	}
	if base.IsLoggingEnabled() {
		toolbox.Dump(job)
	}
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Insert(projectID, job)
	call.Context(ctx)
	if job, err = call.Do(); err == nil {
		return job, err
	}
	if base.IsBackendError(err.Error()) {
		return call.Do()
	}
	return job, err
}
