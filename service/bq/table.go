package bq

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
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

	for i := 0; i < shared.MaxRetries; i++ {
		call.Context(ctx)
		if table, err = call.Do(); err == nil {
			return table, err
		}
		if base.IsRetryError(err) {
			//do extra sleep before retrying
			time.Sleep(shared.RetrySleepInSec * time.Second)
			continue
		}
		err = errors.Wrapf(err, "failed to lookup table schema: %v:%v.%v", reference.ProjectId, reference.DatasetId, tableID)
		break
	}
	return table, err
}

//CreateTableIfNotExist creates a table if does not exist
func (s *service) CreateTableIfNotExist(ctx context.Context, table *bigquery.Table) error {
	ref := table.TableReference
	srv := bigquery.NewTablesService(s.Service)
	if ref.ProjectId == "" {
		ref.ProjectId = s.ProjectID
	}
	getTableCall := srv.Get(ref.ProjectId, ref.DatasetId, ref.TableId)
	getTableCall.Context(ctx)
	_, err := getTableCall.Do()
	if ! base.IsNotFoundError(err) {
		return nil
	}
	insertTableCall := srv.Insert(ref.ProjectId, ref.DatasetId, table)
	insertTableCall.Context(ctx)
	_, err = insertTableCall.Do()
	return err
}

