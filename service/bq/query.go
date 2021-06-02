package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Query run supplied SQL
func (s *service) Query(ctx context.Context, request *QueryRequest, action *task.Action) (*bigquery.Job, error) {
	if err := request.Init(s.projectID, action); err != nil {
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
				DestinationTable: request.destinationTable,
			},
		},
	}

	if job.Configuration.Query.DestinationTable != nil {
		s.createFromTemplate(ctx, request.Template, job.Configuration.Query.DestinationTable)
		if request.Append {
			job.Configuration.Query.WriteDisposition = "WRITE_APPEND"
		} else {
			job.Configuration.Query.WriteDisposition = "WRITE_TRUNCATE"
		}
		s.adjustRegion(ctx, action, job.Configuration.Query.DestinationTable)
	}

	if !request.IsSelectQuery() {
		job.Configuration.Query.WriteDisposition = ""
		job.Configuration.Query.DestinationTable = nil
	}

	if request.UseLegacy {
		job.Configuration.Query.AllowLargeResults = true
	}

	if request.DatasetID != "" {
		job.Configuration.Query.DefaultDataset = &bigquery.DatasetReference{
			DatasetId: request.DatasetID,
			ProjectId: action.Meta.ProjectID,
		}
	}
	job.JobReference = action.JobReference()

	if shared.IsInfoLoggingLevel() {
		source := job.Configuration.Query.Query
		if len(source) > 40 {
			source = strings.Replace(string(source[:40]), "\n", "", len(source))
		}
		dest := ""
		if job.Configuration.Query.DestinationTable != nil {
			dest = base.EncodeTableReference(job.Configuration.Query.DestinationTable, true)
		}
		shared.LogF("[%v] running query %v ... into %v\n", action.Meta.DestTable, source, dest)
	}
	return s.Post(ctx, job, action)
}

//QueryRequest represents Query request
type QueryRequest struct {
	DatasetID        string
	SQL              string
	SQLURL           string
	UseLegacy        bool
	Append           bool
	Dest             string
	Template         string
	destinationTable *bigquery.TableReference
}

//IsSelectQuery returns true if SELECT query
func (r *QueryRequest) IsSelectQuery() bool {
	SQL := strings.ToUpper(strings.TrimSpace(r.SQL))
	if strings.HasPrefix(SQL, "SELECT") || strings.HasPrefix(SQL, "WITH") {
		return  true
	}
	isDML := strings.Contains(SQL, "INSERT ") || strings.Contains(SQL, "UPDATE ") || strings.Contains(SQL, "DELETE ") || strings.Contains(SQL, "MERGE ")
	return !isDML
}


//Init initialises request
func (r *QueryRequest) Init(projectID string, Action *task.Action) (err error) {
	Action.Meta.GetOrSetProject(projectID)
	if r.Dest != "" {
		if r.destinationTable, err = base.NewTableReference(r.Dest); err != nil {
			return err
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
func (r *QueryRequest) Validate() error {
	if r.SQL == "" {
		return fmt.Errorf("SQL was empty")
	}
	return nil
}

//NewQueryAction creates a new query request
func NewQueryAction(SQL string, dest *bigquery.TableReference, template string, append bool, finally *task.Actions) *task.Action {
	query := &QueryRequest{
		SQL:              SQL,
		destinationTable: dest,
		Append:           append,
		Template:         template,
	}
	if dest != nil {
		query.Dest = base.EncodeTableReference(dest, false)
	}
	result := &task.Action{
		Action:  shared.ActionQuery,
		Actions: finally,
		Meta:    nil,
	}
	_ = result.SetRequest(query)
	return result
}

//NewDMLAction creates a new DML query request
func NewDMLAction(SQL string, dest *bigquery.TableReference, template string, append bool, finally *task.Actions) *task.Action {
	query := &QueryRequest{
		SQL:              SQL,
		destinationTable: dest,
		Append:           append,
		Template:         template,
	}
	if dest != nil {
		query.Dest = base.EncodeTableReference(dest, false)
	}
	result := &task.Action{
		Action:  shared.ActionQuery,
		Actions: finally,
		Meta:    nil,
	}
	_ = result.SetRequest(query)
	return result
}
