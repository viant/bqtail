package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//GetJob returns a job ID
func (s *service) GetJob(ctx context.Context, projectID, jobID string) (*bigquery.Job, error) {
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Get(projectID, jobID)
	call.Context(ctx)
	return call.Do()
}



//GetJob returns a job ID
func (s *service) ListJob(ctx context.Context, projectID string, minCreateTime time.Time, stateFilter ... string) ([]*bigquery.JobListJobs, error) {
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.List(projectID)
	call.MinCreationTime(uint64(minCreateTime.Unix() * 1000))
	call.StateFilter(stateFilter...)
	result := make([]*bigquery.JobListJobs, 0)
	pageToken := ""
	for  ;; {
		call.Context(ctx)
		call.PageToken(pageToken)
		list, err := call.Do()
		if err != nil {
			return nil, err
		}
		result = append(result, list.Jobs...)
		if pageToken = list.NextPageToken; pageToken == "" {
			break
		}
	}
	return result, nil
}
