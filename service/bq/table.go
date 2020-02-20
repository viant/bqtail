package bq

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
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

func (s *service) createTableIfNeeded(ctx context.Context, actionable *task.Action, ref *bigquery.TableReference) {
	if actionable.Meta.Region != "" {
		return
	}
	//read dest dataset location
	datasetCall := s.Service.Datasets.Get(ref.ProjectId, ref.DatasetId)
	datasetCall.Context(ctx)
	if dataset, err := datasetCall.Do(); err == nil {
		actionable.Meta.Region = dataset.Location
	}
}
