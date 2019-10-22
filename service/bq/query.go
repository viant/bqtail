package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Query run supplied SQL
func (s *service) Query(ctx context.Context, request *QueryRequest) (*bigquery.Job, error) {
	if err := request.Init(s.projectID); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &bigquery.JobConfigurationQuery{
				Query:            request.SQL,
				UseLegacySql:     &request.UseLegacy,
				DestinationTable: request.destTable,
			},
		},
	}

	if job.Configuration.Query.DestinationTable != nil {
		if request.Append {
			job.Configuration.Query.WriteDisposition = "WRITE_APPEND"
		} else {
			job.Configuration.Query.WriteDisposition = "WRITE_TRUNCATE"
		}

	}

	if request.UseLegacy {
		job.Configuration.Query.AllowLargeResults = true
	}

	if request.DatasetID != "" {
		job.Configuration.Query.DefaultDataset = &bigquery.DatasetReference{
			DatasetId: request.DatasetID,
			ProjectId: request.ProjectID,
		}
	}
	job.JobReference = request.jobReference()
	return s.Post(ctx, request.ProjectID, job, &request.Actions)
}

//QueryRequest represents Query request
type QueryRequest struct {
	DatasetID string
	SQL       string
	UseLegacy bool
	Append    bool
	Dest      string
	destTable *bigquery.TableReference
	Request
}

//Init initialises request
func (r *QueryRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}
	if r.Dest != "" {
		if r.destTable, err = base.NewTableReference(r.Dest); err != nil {
			return err
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
func (r *QueryRequest) Validate() error {
	if r.SQL == "" {
		return fmt.Errorf("SQL was empty")
	}
	return nil
}

//NewQueryRequest creates a new query request
func NewQueryRequest(SQL string, dest *bigquery.TableReference, finally *task.Actions) *QueryRequest {
	result := &QueryRequest{
		SQL:       SQL,
		destTable: dest,
	}
	if dest != nil {
		result.Dest = base.EncodeTableReference(dest)
	}
	if finally != nil {
		result.Actions = *finally
	}
	return result
}
