package bq

import (
	"bqtail/base"
	"context"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Table returns bif query table
func (s *service) Table(ctx context.Context, reference *bigquery.TableReference) (table *bigquery.Table, err error) {
	if reference.ProjectId == "" {
		reference.ProjectId = s.projectID
	}
	tableID := base.TableID(reference.TableId)
	call := bigquery.NewTablesService(s.Service).Get(reference.ProjectId, reference.DatasetId, tableID)

	for i := 0; i < base.MaxRetries; i++ {
		call.Context(ctx)
		if table, err = call.Do(); err == nil {
			return table, err
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(base.RetrySleepInSec * time.Second)
			continue
		}
		break
	}
	return table, err
}
