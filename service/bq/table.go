package bq

import (
	"context"
	"google.golang.org/api/bigquery/v2"
)

//Table returns bif query table
func (s *service) Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error) {
	if reference.ProjectId == "" {
		reference.ProjectId = s.projectID
	}
	call := bigquery.NewTablesService(s.Service).Get(reference.ProjectId, reference.DatasetId, reference.TableId)
	call.Context(ctx)
	return call.Do()
}
