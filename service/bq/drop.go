package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Drop drop source table
func (s *service) Drop(ctx context.Context, request *DropRequest, action *task.Action) error {
	if err := request.Init(s.projectID); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	table := request.dropTable
	call := bigquery.NewTablesService(s.Service).Delete(table.ProjectId, table.DatasetId, table.TableId)
	var err error
	for i := 0; i < shared.MaxRetries; i++ {
		call.Context(ctx)
		if err = call.Do(); err == nil {
			return err
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(shared.RetrySleepInSec * time.Second)
			continue
		}
		break
	}
	return err
}



//DropRequest represents a copy request
type DropRequest struct {
	ProjectID string
	Table     string
	dropTable *bigquery.TableReference
}

//Init initialises a copy request
func (r *DropRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	}
	if r.Table != "" {
		if r.dropTable, err = base.NewTableReference(r.Table); err != nil {
			return err
		}
	}
	if r.dropTable != nil && r.dropTable.ProjectId == "" {
		r.dropTable.ProjectId = projectID
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

//NewDropAction creates a new drop request
func NewDropAction(projectID string, table string) *task.Action {
	drop := &DropRequest{
		ProjectID: projectID,
		Table:     table,
	}
	if table != "" {
		drop.dropTable, _ = base.NewTableReference(table)
	}
	result := &task.Action{
		Action: shared.ActionDrop,
	}
	_ = result.SetRequest(drop)
	return result
}
