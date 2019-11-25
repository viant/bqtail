package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"github.com/viant/afs"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Service represents big query service
type Service interface {
	task.Service

	GetJob(ctx context.Context, projectID, jobID string) (*bigquery.Job, error)

	ListJob(ctx context.Context, projectID string, minCreateTime time.Time, stateFilter ...string) ([]*bigquery.JobListJobs, error)

	Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error)

	Load(ctx context.Context, request *LoadRequest) (*bigquery.Job, error)

	Query(ctx context.Context, request *QueryRequest) (*bigquery.Job, error)

	Wait(ctx context.Context, ref *bigquery.JobReference) (*bigquery.Job, error)
}

type service struct {
	base.Config
	*bigquery.Service
	Registry  task.Registry
	jobs      *bigquery.JobsService
	projectID string
	fs        afs.Service
}

//New creates bq service
func New(bq *bigquery.Service, registry task.Registry, projectID string, storageService afs.Service, config base.Config) Service {
	return &service{
		Service:   bq,
		Config:    config,
		Registry:  registry,
		projectID: projectID,
		fs:        storageService,
	}
}
