package bq

import (
	"bqtail/base"
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
				DestinationTable: request.destTable,
			},
		},
	}
	job.JobReference = request.jobReference()
	return s.Post(ctx, request.ProjectID, job, &request.Actions)
}

//CopyRequest represents a copy request
type CopyRequest struct {
	Request
	Append      bool
	Source      string
	sourceTable *bigquery.TableReference
	Dest        string
	destTable   *bigquery.TableReference
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
		if r.destTable, err = base.NewTableReference(r.Dest); err != nil {
			return err
		}
	}
	if r.sourceTable != nil {
		if r.sourceTable.ProjectId == "" {
			r.sourceTable.ProjectId = projectID
		}
	}
	if r.destTable != nil {
		if r.destTable.ProjectId == "" {
			r.destTable.ProjectId = projectID
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *CopyRequest) Validate() error {
	if r.sourceTable == nil {
		return fmt.Errorf("sourceTable was empty")
	}
	if r.destTable == nil {
		return fmt.Errorf("destTable was empty")
	}
	return nil
}
