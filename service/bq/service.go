package bq

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Service represents big query service
type Service interface {
	task.Service

	GetJob(ctx context.Context, location, projectID, jobID string) (*bigquery.Job, error)

	ListJob(ctx context.Context, projectID string, minCreateTime, maxCreateTime time.Time, stateFilter ...string) ([]*bigquery.JobListJobs, error)

	Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error)

	Load(ctx context.Context, request *LoadRequest, Action *task.Action) (*bigquery.Job, error)

	Query(ctx context.Context, request *QueryRequest, Action *task.Action) (*bigquery.Job, error)

	Wait(ctx context.Context, ref *bigquery.JobReference) (*bigquery.Job, error)

	CreateDatasetIfNotExist(ctx context.Context, region string, dataset *bigquery.DatasetReference) error

	CreateTableIfNotExist(ctx context.Context, table *bigquery.Table) error
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
