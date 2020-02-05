package bq

import (
	"bqtail/base"
	"bqtail/shared"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Patch patch temp table
func (s *service) Patch(ctx context.Context, request *PatchRequest) (*bigquery.Table, error) {
	if err := request.Init(s.projectID); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	tableRef, err := base.NewTableReference(request.Table)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid table: %v", request.Table)
	}
	if tableRef.ProjectId == "" {
		tableRef.ProjectId = s.ProjectID
	}
	templateRef, err := base.NewTableReference(request.Template)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid template table: %v", request.Template)
	}
	schema, err := s.Table(ctx, templateRef)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid get template table: %v", request.Table)
	}

	call := s.Service.Tables.Patch(tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId, schema)
	call.Context(ctx)

	var table *bigquery.Table
	for i := 0; i < shared.MaxRetries; i++ {
		call.Context(ctx)
		if table, err = call.Do(); err == nil {
			return nil, err
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(shared.RetrySleepInSec * time.Second)
			continue
		}
		break
	}
	return table, err
}

//PatchRequest represents an export request
type PatchRequest struct {
	Template string
	Table    string
	Request
}

//Init initialises request
func (r *PatchRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}
	return nil
}

//Validate checks if request is valid
func (r *PatchRequest) Validate() error {
	if r.Template == "" {
		return fmt.Errorf("template was empty")
	}
	if r.Table == "" {
		return fmt.Errorf("destination was empty")
	}
	return nil
}

//NewPatchRequest creates a new patch request
func NewPatchRequest(template, table string, finally *task.Actions) *PatchRequest {
	result := &PatchRequest{
		Table:    table,
		Template: template,
	}
	if finally != nil {
		result.Actions = *finally
	}
	return result
}
