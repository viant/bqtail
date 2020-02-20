package bq

import (
	"context"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) adjustRegion(ctx context.Context, actionable *task.Action, ref *bigquery.TableReference) {
	if actionable.Meta.Region != "" {
		return
	}
	//read dest dataset location
	datasetCall := s.Service.Datasets.Get(ref.ProjectId, ref.DatasetId)
	datasetCall.Context(ctx)
	if dataset, err := datasetCall.Do(); err == nil {
		actionable.Meta.Region = dataset.Location
	}
}

//CreateDatasetIfNotExist cretes a dataset if does not exist
func (s *service) CreateDatasetIfNotExist(ctx context.Context, region string, dataset *bigquery.DatasetReference) error {
	//read dest dataset location
	if region == "" {
		region = defaultRegion
	}
	if dataset.ProjectId == ""{
		dataset.ProjectId = s.projectID
	}
	datasetCall := s.Service.Datasets.Get(dataset.ProjectId, dataset.DatasetId)
	datasetCall.Context(ctx)
	_, err := datasetCall.Do()
	if ! base.IsNotFoundError(err) {
		return err
	}
	insertDatasetCall := s.Service.Datasets.Insert(dataset.ProjectId, &bigquery.Dataset{
		DatasetReference: dataset,
		Location:         region,
	})
	insertDatasetCall.Context(ctx)
	_, err = insertDatasetCall.Do()
	return err
}
