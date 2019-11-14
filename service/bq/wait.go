package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const (
	doneStatus = "DONE"
)

//Wait waits for job completion
func (s *service) Wait(ctx context.Context, ref *bigquery.JobReference) (*bigquery.Job, error) {
	var err error
	var statusJob *bigquery.Job
	for {
		if statusJob, err = s.GetJob(ctx, ref.ProjectId, ref.JobId); err != nil {
			return nil, err
		}
		if statusJob.Status.State == doneStatus {
			break
		}
		time.Sleep(time.Second)
	}
	return statusJob, err
}

//waitWithTimeout waits for job completion
func (s *service) waitWithTimeout(ctx context.Context, ref *bigquery.JobReference, timeout time.Duration) (*bigquery.Job, error) {
	var err error
	started := time.Now()
	var statusJob *bigquery.Job
	for {
		if statusJob, err = s.GetJob(ctx, ref.ProjectId, ref.JobId); err != nil {
			return nil, err
		}
		if statusJob.Status.State == doneStatus {
			break
		}
		if time.Now().Sub(started) > timeout {
			return statusJob, nil
		}
		time.Sleep(time.Second)

	}
	return statusJob, err
}
