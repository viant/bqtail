package bq

import (
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//GetJob returns a job ID
func (s *service) GetJob(ctx context.Context, projectID, jobID string) (*bigquery.Job, error) {
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Get(projectID, jobID)
	call.Context(ctx)
	return call.Do()
}

//JobError check job status and returns error or nil
func JobError(job *bigquery.Job) error {
	if job.Status.ErrorResult != nil {
		return fmt.Errorf("failed to run job: %s", job.Status.ErrorResult.Message)
	}
	return nil
}
