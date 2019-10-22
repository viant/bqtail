package bq

import (
	"bqtail/base"
	"context"
	"github.com/pkg/errors"
	"google.golang.org/api/bigquery/v2"
)

//Table returns bif query table
func (s *service) Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error) {
	if reference.ProjectId == "" {
		reference.ProjectId = s.projectID
	}
	tableId := base.TableID(reference.TableId)
	call := bigquery.NewTablesService(s.Service).Get(reference.ProjectId, reference.DatasetId, tableId)
	call.Context(ctx)
	table, err := call.Do()
	if err != nil {
		err = errors.Wrapf(err, "failed to get scheme for [%v:%v.%v]", reference.ProjectId, reference.DatasetId, tableId)
	}
	return table, err
}
