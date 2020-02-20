package bq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)

func (s *service) setJobID(action *task.Action) (*bigquery.JobReference, error) {
	ID := action.Meta.GetJobID()
	projectID := action.Meta.GetOrSetProject(s.Config.ProjectID)
	return &bigquery.JobReference{
		Location:  action.Meta.Region,
		JobId:     ID,
		ProjectId: projectID,
	}, nil
}

func (s *service) schedulePostTask(ctx context.Context, job *bigquery.Job, action *task.Action) error {
	if action.IsEmpty() || action.Meta.IsSyncMode() {
		return nil
	}
	action.Job = job
	data, err := json.Marshal(action)
	if err != nil {
		return errors.Wrapf(err, "failed to encode actions: %v", action)
	}
	filename := action.Meta.JobFilename()
	URL := url.Join(s.Config.AsyncTaskURL, filename)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

//Post post big query job
func (s *service) Post(ctx context.Context, callerJob *bigquery.Job, action *task.Action) (*bigquery.Job, error) {
	job, err := s.post(ctx, callerJob, action)
	if job == nil {
		job = callerJob
	} else {
		callerJob.Id = job.Id
	}
	if base.IsLoggingEnabled() {
		base.Log(action)
	}

	if action.Meta.IsSyncMode() {
		err = base.JobError(job)
		if err == nil && !base.IsJobDone(job) {
			job, err = s.Wait(ctx, job.JobReference)
			if err == nil {
				err = base.JobError(job)
			}
		}
		if base.IsLoggingEnabled() && job != nil && job.Status != nil {
			base.Log(job.Status)
		}
		if job == nil {
			job = callerJob
		}
		if e := s.runActions(ctx, err, job, action.Actions); e != nil {
			if err == nil {
				err = e
			} else {
				err = errors.Wrapf(err, "failed to run post action: %v", e)
			}
		}
	}

	if bqErr := base.JobError(job); bqErr != nil {
		errorURL := url.Join(s.ErrorURL, action.Meta.DestTable, fmt.Sprintf("%v%v", action.Meta.EventID, shared.ErrorExt))
		_ = s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(bqErr.Error()))
	}
	return job, err
}

func (s *service) post(ctx context.Context, job *bigquery.Job, action *task.Action) (*bigquery.Job, error) {
	var err error
	if job.JobReference, err = s.setJobID(action); err != nil {
		return nil, err
	}
	err = s.schedulePostTask(ctx, job, action)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to schedule bqJob %v", job.JobReference.JobId)
	}

	if base.IsLoggingEnabled() {
		base.Log(job)
	}
	projectID := action.Meta.GetOrSetProject(s.Config.ProjectID)
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Insert(projectID, job)
	call.Context(ctx)
	var callJob *bigquery.Job
	for i := 0; i < shared.MaxRetries; i++ {
		if callJob, err = call.Do(); err == nil {
			break
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(shared.RetrySleepInSec * time.Second)
			continue
		}
		if i > 0 && base.IsDuplicateJobError(err) {
			if base.IsLoggingEnabled() {
				fmt.Printf("duplicate job: [%v]: %v\n", job.Id, err)
			}
			err = nil
			callJob, _ = s.GetJob(ctx, job.JobReference.Location, job.JobReference.ProjectId, job.JobReference.JobId)
			break
		}
		if err != nil {
			detail, _ := json.Marshal(job)
			err = errors.Wrapf(err, "failed to submit: %T %s", call, detail)
		}
	}
	if err != nil || (callJob != nil && base.JobError(callJob) != nil) {
		if base.IsLoggingEnabled() && callJob != nil && callJob.Status != nil {
			base.Log(callJob.Status)
		}
		return callJob, err
	}
	return s.GetJob(ctx, job.JobReference.Location, job.JobReference.ProjectId, job.JobReference.JobId)
}
