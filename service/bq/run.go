package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) Run(ctx context.Context, request task.Request) error {
	var job *bigquery.Job
	var err error
	switch req := request.(type) {
	case *CopyRequest:
		job, err = s.Copy(ctx, req)

	case *ExportRequest:
		job, err = s.Export(ctx, req)

	case *QueryRequest:
		job, err = s.Query(ctx, req)

	case *LoadRequest:
		job, err = s.Load(ctx, req)
	default:
		return fmt.Errorf("unsupported request type:%T", request)
	}
	if err != nil {
		return err
	}
	return base.JobError(job)
}
