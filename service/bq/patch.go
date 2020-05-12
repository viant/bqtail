package bq

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"google.golang.org/api/bigquery/v2"
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
	if request.TemplateTable == nil {
		templateRef, err := base.NewTableReference(request.Template)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid template table: %v", request.Template)
		}
		request.TemplateTable, err = s.Table(ctx, templateRef)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid get template table: %v", request.Table)
		}
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogF("patching table: %+v\n", tableRef)
	}

	var table *bigquery.Table
	call := s.Service.Tables.Patch(tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId, request.TemplateTable)
	call.Context(ctx)
	err = base.RunWithRetriesOnRetryOrInternalError(func() error {
		table, err = call.Do()
		return err
	})
	return table, err
}

//PatchRequest represents an export request
type PatchRequest struct {
	Template      string
	Table         string
	TemplateTable *bigquery.Table
	ProjectID     string
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
	if r.Template == "" && r.TemplateTable == nil {
		return fmt.Errorf("template was empty")
	}
	if r.Table == "" {
		return fmt.Errorf("destination was empty")
	}
	return nil
}
