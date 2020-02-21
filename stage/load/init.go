package load

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/shared"
	"google.golang.org/api/bigquery/v2"
)

//Init initialises job
func (j *Job) Init(ctx context.Context, service bq.Service) error {
	tableReference, err := base.NewTableReference(j.DestTable)
	if err != nil {
		return err
	}
	if err = j.updateSchemaIfNeeded(ctx, tableReference, service); err != nil {
		return err
	}

	j.setDestinationTable(tableReference)
	j.Actions, err = j.buildActions()
	if err != nil {
		return errors.Wrapf(err, "failed to build actions")
	}
	if j.Rule.Dest.HasSplit() {
		if err = j.applySplitSchemaOptimization(); err != nil {
			return errors.Wrapf(err, "failed to apply split schema optimization: %+v", j.Rule.Dest.Schema.Split)
		}
	}
	return nil
}

func (j *Job) setDestinationTable(tableReference *bigquery.TableReference) {
	if j.Rule.Dest.Transient != nil {
		tableReference.ProjectId = j.ProjectID
		tableReference.DatasetId = j.Rule.Dest.Transient.Dataset
		tableReference.TableId = base.TableID(tableReference.TableId) + "_" + j.EventID
		j.Load.WriteDisposition = shared.WriteDispositionTruncate
		j.TempTable = "`" + base.EncodeTableReference(tableReference, true) + "`"
	} else {
		if j.Rule.IsAppend() {
			j.Load.WriteDisposition = shared.WriteDispositionAppend
		} else {
			j.Load.WriteDisposition = shared.WriteDispositionTruncate
		}
	}
	j.Load.DestinationTable = tableReference
}
