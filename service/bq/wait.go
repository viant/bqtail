package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const (
	doneStatus = "DONE"
)

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