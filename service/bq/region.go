package bq

import (
	"context"
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
