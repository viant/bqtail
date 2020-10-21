package bq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

const maxColumns = 90

type InsertRequest struct {
	ProjectId string
	Dest      string

	Template string
	Data     []map[string]bigquery.JsonValue
	SQL      string
	UseLegacy bool
}

//Insert insert data into BigQuery with streaming API
func (s *service) Insert(ctx context.Context, request *InsertRequest, action *task.Action) (response *bigquery.TableDataInsertAllResponse, err error) {
	tableRef, err := base.NewTableReference(request.Dest)
	if err != nil {
		return nil, err
	}
	s.initInsertRequest(action, request, tableRef)

	if request.SQL != "" {
		if request.Data, err = s.fetchAll(ctx, request.ProjectId, request.UseLegacy,  request.SQL); err != nil {
			return nil, err
		}
	}
	if len(request.Data) == 0 {
		return nil, nil
	}
	if shared.IsInfoLoggingLevel() {
		message := fmt.Sprintf("insert into %v AS %v", request.Dest, request.SQL)
		if len(message) > maxColumns {
			message = message[:maxColumns] + "..."
		}
		shared.LogLn(message)
	}
	err = s.createFromTemplate(ctx, request.Template, tableRef)
	if err != nil {
		return nil, err
	}
	rows := []*bigquery.TableDataInsertAllRequestRows{}

	for _, item := range request.Data {
		rows = append(rows, &bigquery.TableDataInsertAllRequestRows{Json: item})
	}
	insertRequest := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}
	requestCall := s.Tabledata.InsertAll(tableRef.ProjectId, tableRef.DatasetId, tableRef.TableId, insertRequest)
	requestCall.Context(ctx)
	err = base.RunWithRetries(func() error {
		response, err = requestCall.Do()
		return err
	})
	if response != nil && len(response.InsertErrors) > 0 {
		JSON, _ := json.Marshal(response.InsertErrors)
		err = errors.Errorf("failed to stream to %v, %s", request.Dest, JSON)
	}
	return response, err
}

func (s *service) initInsertRequest(action *task.Action, request *InsertRequest, tableRef *bigquery.TableReference) {
	if action != nil && request.ProjectId == "" {
		request.ProjectId = action.Meta.GetOrSetProject(s.Config.ProjectID)
	}
	if request.ProjectId == "" {
		request.ProjectId = tableRef.ProjectId
	}
	if tableRef.ProjectId == "" {
		tableRef.ProjectId = request.ProjectId
	}
}
