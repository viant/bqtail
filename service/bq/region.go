package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) adjustRegion(ctx context.Context, request *Request, ref *bigquery.TableReference) {
	if request.Region != "" {
		return
	}
	//read dest dataset location
	datasetCall := s.Service.Datasets.Get(ref.ProjectId, ref.DatasetId)
	datasetCall.Context(ctx)
	if dataset, err := datasetCall.Do(); err == nil {
		request.Region = dataset.Location
	}
}
