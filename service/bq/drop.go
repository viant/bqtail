package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Drop drop source table
func (s *service) Drop(ctx context.Context, request *DropRequest) error {
	if err := request.Init(s.projectID); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	table := request.dropTable
	call := bigquery.NewTablesService(s.Service).Delete(table.ProjectId, table.DatasetId, table.TableId)
	call.Context(ctx)
	err := call.Do()
	if base.IsNotFoundError(err) {
		err = nil
	}
	return err
}

//DropRequest represents a copy request
type DropRequest struct {
	Request
	Table     string
	dropTable *bigquery.TableReference
}

//Init initialises a copy request
func (r *DropRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}
	if r.Table != "" {
		if r.dropTable, err = base.NewTableReference(r.Table); err != nil {
			return err
		}
	}
	if r.dropTable != nil && r.dropTable.ProjectId == "" {
		r.dropTable.ProjectId = r.ProjectID
	}
	return nil
}

//Validate checks if request is valid
func (r *DropRequest) Validate() error {
	if r.dropTable == nil {
		return fmt.Errorf("table was empty")
	}
	return nil
}

//NewDropRequest creates a new drop request
func NewDropRequest(table string, finally *task.Actions) *DropRequest {
	result := &DropRequest{
		Table: table,
	}
	if table != "" {
		result.dropTable, _ = base.NewTableReference(table)
	}
	if finally != nil {
		result.Actions = *finally
	}
	return result
}
