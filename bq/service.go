package bq

import (
	"bqtail/gcp"
	"context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const doneStatus = "DONE"

type Service interface {
	Load(*LoadRequest) *LoadResponse
}

type service struct {
	credentials string
	bqService   *gcp.BigQueryService
}

func (s *service) getJob(ctx context.Context, jobService *bigquery.JobsService, ref *bigquery.JobReference) (*bigquery.Job, error) {
	getCall := jobService.Get(ref.ProjectId, ref.JobId)
	getCall.Context(ctx)
	return getCall.Do()
}

func (s *service) Load(request *LoadRequest) *LoadResponse {
	response := &LoadResponse{}
	if err := s.load(request, response); err != nil {
		response.Error = err.Error()
	}
	return response
}

func (s *service) getBQService() (*gcp.BigQueryService, error) {
	if s.bqService != nil {
		return s.bqService, nil
	}
	client, err := gcp.NewBigQueryClient()
	if err != nil {
		return nil, err
	}
	s.bqService = client
	return client, err
}

func (s *service) load(request *LoadRequest, response *LoadResponse) error {
	bqService, err := s.getBQService()
	if err != nil {
		return err
	}
	bigquery.NewJobsService(bqService.Service)
	request.Init()

	client, err := gcp.NewBigQueryClient()
	if err != nil {
		return err
	}
	jobService := bigquery.NewJobsService(client.Service)

	insertCall := jobService.Insert(request.DestinationTable.ProjectId, &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &request.JobConfigurationLoad,
		},
	})
	job, err := insertCall.Do()
	if err != nil {
		return err
	}
	if request.Wait {
		job, err = s.waitForDone(client.Context, jobService, job.JobReference)
	}
	if err != nil {
		return err
	}
	response.Job = job
	return nil

}

func (s *service) waitForDone(ctx context.Context, jobService *bigquery.JobsService, ref *bigquery.JobReference) (*bigquery.Job, error) {
	var err error
	var statusJob *bigquery.Job
	for {
		if statusJob, err = s.getJob(ctx, jobService, ref); err != nil {
			return nil, err
		}
		if statusJob.Status.State == doneStatus {
			break
		}
		time.Sleep(time.Second)
	}
	return statusJob, err
}

func New(credentials string) Service {
	return &service{
		credentials: credentials,
	}
}
