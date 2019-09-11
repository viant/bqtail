package bq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"bqtail/base"
	"bqtail/task"
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
		return err
	}
	filename := jobReference.JobId
	if path.Ext(filename) == "" {
		filename += base.JobExt
	}
	URL := url.Join(actions.DispatchURL, filename)
	return s.storage.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}


func (s *service) Post(ctx context.Context, projectID string, job *bigquery.Job, onDone *task.Actions) (*bigquery.Job, error) {
	job, err := s.post(ctx, projectID, job, onDone)
	toolbox.Dump(job)

	if err == nil {
		err = JobError(job)
	}
	if onDone != nil && onDone.IsSyncMode() {
		if err == nil {
			job, err = s.Wait(ctx, job.JobReference)
			if err == nil {
				err = JobError(job)
			}
		}
		if e := s.runActions(ctx, err, job, onDone); e != nil {
			if err == nil {
				err = e
			}
		}
	}
	return job, err
}

func (s *service) post(ctx context.Context, projectID string, job *bigquery.Job, actions *task.Actions) (*bigquery.Job, error) {
	var err error
	if job.JobReference, err = s.setJobID(ctx, actions); err != nil {
		return nil, err
	}
	if job.JobReference != nil {
		job.JobReference.JobId = base.EncodeID(job.JobReference.JobId)
		fmt.Printf("USING JOB ID:%v\n", job.JobReference.JobId)
	}

	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Insert(projectID, job)
	call.Context(ctx)
	return call.Do()
}
