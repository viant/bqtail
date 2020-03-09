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
	j.setDestinationTable(tableReference)
	tableReference, _ = base.NewTableReference(j.DestTable)
	if err = j.updateSchemaIfNeeded(ctx, tableReference, service); err != nil {
		return err
	}

	if j.Rule.Dest.HasSplit() {
		if err = j.initTableSplit(ctx, service); err != nil {
			return errors.Wrapf(err, "failed to apply split schema optimization: %+v", j.Rule.Dest.Schema.Split)
		}
	}
	j.Actions, err = j.buildActions()
	if err != nil {
		return errors.Wrapf(err, "failed to build actions")
	}

	if j.Rule.Dest.HasTemplate() {
		j.Load.Schema = nil
	}
	return nil
}

func (j *Job) setDestinationTable(tableReference *bigquery.TableReference) {
	if j.Rule.Dest.Transient != nil {
		tableReference.ProjectId = j.ProjectID
		tableReference.DatasetId = j.Rule.Dest.Transient.Dataset
		tableReference.TableId = base.TableID(tableReference.TableId) + "_" + j.EventID
		j.setWriteDispositionIfNotSet(shared.WriteDispositionTruncate)
		j.TempTable = "`" + base.EncodeTableReference(tableReference, true) + "`"
	} else {
		if j.Rule.IsAppend() {
			j.setWriteDispositionIfNotSet(shared.WriteDispositionAppend)
		} else {
			j.setWriteDispositionIfNotSet(shared.WriteDispositionTruncate)
		}
	}
	j.Load.DestinationTable = tableReference
}

func (j *Job) setWriteDispositionIfNotSet(disposition string) {
	if j.Load.WriteDisposition != "" {
		return
	}
	j.Load.WriteDisposition = disposition
}
