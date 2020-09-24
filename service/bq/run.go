package bq

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

//Run run request
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	var job *bigquery.Job
	var err error
	serviceRequest := request.ServiceRequest()
	switch req := serviceRequest.(type) {
	case *CopyRequest:
		job, err = s.Copy(ctx, req, request)
	case *ExportRequest:
		job, err = s.Export(ctx, req, request)
	case *DropRequest:
		err = s.Drop(ctx, req, request)
	case *QueryRequest:
		job, err = s.Query(ctx, req, request)
	case *LoadRequest:
		job, err = s.Load(ctx, req, request)
	case *InsertRequest:
		_, err = s.Insert(ctx, req, request)
	case *TableExistsRequest:
		return s.TableExists(ctx, req)
	default:
		return nil, errors.Errorf("unsupported request type:%T", request)
	}
	if err != nil || job == nil {
		return nil, err
	}
	return job, base.JobError(job)
}
