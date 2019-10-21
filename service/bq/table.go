package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Table returns bif query table
func (s *service) Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error) {
	if reference.ProjectId == "" {
		reference.ProjectId = s.projectID
	}
	tableId := reference.TableId
	if index:=strings.Index(tableId, "$");index !=-1{
		tableId = string(tableId[:index])
	}
	call := bigquery.NewTablesService(s.Service).Get(reference.ProjectId, reference.DatasetId, tableId)
	call.Context(ctx)
	return call.Do()
}
