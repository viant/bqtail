package bq

import (
	"bqtail/task"
	"context"
	"github.com/viant/afs"
	"google.golang.org/api/bigquery/v2"
)

//Service represents big query service
type Service interface {
	task.Service

	GetJob(ctx context.Context, projectID, jobID string) (*bigquery.Job, error)

	Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error)

	Load(ctx context.Context, request *LoadRequest) (*bigquery.Job, error)

	Query(ctx context.Context, request *QueryRequest) (*bigquery.Job, error)

	Wait(ctx context.Context, ref *bigquery.JobReference) (*bigquery.Job, error)
}

type service struct {
	deferTaskURL string
	*bigquery.Service
	Registry  task.Registry
	jobs      *bigquery.JobsService
	projectID string
	storage   afs.Service
}

//New creates bq service
func New(bq *bigquery.Service, registry task.Registry, projectID string, storageService afs.Service, deferTaskURL string) Service {
	return &service{
		Service:   bq,
		Registry:  registry,
		projectID: projectID,
		storage:   storageService,
	}
}
