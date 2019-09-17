package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
)

//GetJob returns a job ID
func (s *service) GetJob(ctx context.Context, projectID, jobID string) (*bigquery.Job, error) {
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Get(projectID, jobID)
	call.Context(ctx)
	return call.Do()
}
