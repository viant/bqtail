package bq

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Copy copy source to dest table
func (s *service) Copy(ctx context.Context, request *CopyRequest, activity *task.Action) (*bigquery.Job, error) {
	if err := request.Init(s.projectID, activity); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Copy: &bigquery.JobConfigurationTableCopy{
				SourceTable:      request.sourceTable,
				DestinationTable: request.destinationTable,
			},
		},
	}
	if request.Append {
		job.Configuration.Copy.WriteDisposition = "WRITE_APPEND"
	} else {
		job.Configuration.Copy.WriteDisposition = "WRITE_TRUNCATE"
	}
	job.Configuration.Copy.CreateDisposition = "CREATE_IF_NEEDED"

	s.adjustRegion(ctx, activity, job.Configuration.Copy.DestinationTable)
	job.JobReference = activity.JobReference()
	return s.Post(ctx, job, activity)
}

//CopyRequest represents a copy request
type CopyRequest struct {
	Append           bool
	Source           string
	sourceTable      *bigquery.TableReference
	Dest             string
	destinationTable *bigquery.TableReference
}

//Init initialises a copy request
func (r *CopyRequest) Init(projectID string, activity *task.Action) (err error) {
	activity.Meta.GetOrSetProject(projectID)

	if r.Source != "" {
		if r.sourceTable, err = base.NewTableReference(r.Source); err != nil {
			return err
		}
	}
	if r.Dest != "" {
		if r.destinationTable, err = base.NewTableReference(r.Dest); err != nil {
			return err
		}
	}
	if r.sourceTable != nil {
		if r.sourceTable.ProjectId == "" {
			r.sourceTable.ProjectId = projectID
		}
	}
	if r.destinationTable != nil {
		if r.destinationTable.ProjectId == "" {
			r.destinationTable.ProjectId = projectID
		}
	}
	return nil
}


//Validate checks if request is valid
func (r *CopyRequest) Validate() error {
	if r.sourceTable == nil {
		return fmt.Errorf("sourceTable was empty")
	}
	if r.destinationTable == nil {
		return fmt.Errorf("destinationTable was empty")
	}
	return nil
}

//NewCopyAction creates a new copy request
func NewCopyAction(source, dest string, append bool, finally *task.Actions) *task.Action {
	copyRequest := &CopyRequest{
		Source: source,
		Dest:   dest,
		Append: append,
	}
	if source != "" {
		copyRequest.sourceTable, _ = base.NewTableReference(source)
	}
	if dest != "" {
		copyRequest.destinationTable, _ = base.NewTableReference(dest)
	}
	result := &task.Action{
		Action:shared.ActionCopy,
		Actions: finally,
	}
	_ = result.SetRequest(copyRequest)
	return result
}
