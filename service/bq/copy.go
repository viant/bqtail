package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Copy copy source to dest table
func (s *service) Copy(ctx context.Context, request *CopyRequest) (*bigquery.Job, error) {
	if err := request.Init(s.projectID); err != nil {
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
	s.adjustRegion(ctx, &request.Request, job.Configuration.Copy.DestinationTable)
	job.JobReference = request.jobReference()
	return s.Post(ctx, job, &request.Request)
}

//CopyRequest represents a copy request
type CopyRequest struct {
	Request
	Append           bool
	Source           string
	sourceTable      *bigquery.TableReference
	Dest             string
	destinationTable *bigquery.TableReference
}

//Init initialises a copy request
func (r *CopyRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}

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

//NewCopyRequest creates a new copy request
func NewCopyRequest(source, dest string, finally *task.Actions) *CopyRequest {
	result := &CopyRequest{
		Source: source,
		Dest:   dest,
		Append: true,
	}
	if source != "" {
		result.sourceTable, _ = base.NewTableReference(source)
	}
	if dest != "" {
		result.destinationTable, _ = base.NewTableReference(dest)
	}
	if finally != nil {
		result.Actions = *finally
	}
	return result
}
