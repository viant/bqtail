package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"github.com/pkg/errors"
	"google.golang.org/api/bigquery/v2"
)

//Run run request
func (s *service) Run(ctx context.Context, request task.Request) (task.Response, error) {
	var job *bigquery.Job
	var err error
	switch req := request.(type) {
	case *CopyRequest:
		job, err = s.Copy(ctx, req)
	case *ExportRequest:
		job, err = s.Export(ctx, req)
	case *DropRequest:
		err = s.Drop(ctx, req)
	case *QueryRequest:
		job, err = s.Query(ctx, req)
	case *LoadRequest:
		job, err = s.Load(ctx, req)
	default:
		return nil, errors.Errorf("unsupported request type:%T", request)
	}
	if err != nil {
		return nil, err
	}
	return job, base.JobError(job)
}
