package bq

import (
	"bqtail/base"
	"bqtail/shared"
	"bqtail/stage"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)

func (s *service) setJobID(request *Request) (*bigquery.JobReference, error) {
	ID := request.Info.GetJobID()
	projectID := request.ProjectID
	if projectID == "" {
		projectID = s.Config.ProjectID
	}
	return &bigquery.JobReference{
		Location:  request.Region,
		JobId:     ID,
		ProjectId: projectID,
	}, nil
}

func (s *service) schedulePostTask(ctx context.Context, job *bigquery.Job, request *Request) error {
	if request.IsEmpty() || request.IsSyncMode() {
		return nil
	}
	request.Job = job
	data, err := json.Marshal(request.Actions)
	if err != nil {
		return errors.Wrapf(err, "failed to encode actions: %v", request.Actions)
	}
	filename := request.Actions.Info.JobFilename()
	URL := url.Join(s.Config.AsyncTaskURL, filename)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

//Post post big query job
func (s *service) Post(ctx context.Context, callerJob *bigquery.Job, req *Request) (*bigquery.Job, error) {
	if req.JobID != "" && req.Info.Action == "" {
		req.Info = *stage.Parse(req.JobID)
		//Legacy transition
		req.Info.Step = strings.Count(req.JobID, stage.PathElementSeparator)
	}
	job, err := s.post(ctx, callerJob, req)
	if job == nil {
		job = callerJob
	} else {
		callerJob.Id = job.Id
	}
	if base.IsLoggingEnabled() {
		base.Log(job)
	}
	if req.IsSyncMode() {
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
		if e := s.runActions(ctx, err, job, &req.Actions); e != nil {
			if err == nil {
				err = e
			} else {
				err = errors.Wrapf(err, "failed to run post action: %v", e)
			}
		}
	}
	return job, err
}

func (s *service) post(ctx context.Context, job *bigquery.Job, request *Request) (*bigquery.Job, error) {
	var err error
	if job.JobReference, err = s.setJobID(request); err != nil {
		return nil, err
	}
	err = s.schedulePostTask(ctx, job, request)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to schedule bqJob %v", job.JobReference.JobId)
	}

	if base.IsLoggingEnabled() {
		base.Log(job)
	}
	projectID := request.ProjectID
	if projectID == "" {
		projectID = s.Config.ProjectID
	}
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
		}
		if err != nil {
			detail, _ := json.Marshal(job)
			err = errors.Wrapf(err, "failed to submit: %T %s", call, detail)
		}
	}
	if err != nil || (callJob != nil && base.JobError(callJob) != nil) {
		return callJob, err
	}
	return s.GetJob(ctx, job.JobReference.Location, request.ProjectID, job.JobReference.JobId)
}
