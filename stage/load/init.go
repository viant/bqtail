package load

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/shared"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Init initialises job
func (j *Job) Init(ctx context.Context, service bq.Service) (err error) {
	var tableReference *bigquery.TableReference

	if j.DestTable != "" {
		if tableReference, err = base.NewTableReference(j.DestTable); err != nil {
			return err
		}
		j.setDestinationTable(tableReference)
		tableReference, _ = base.NewTableReference(j.DestTable)
		if err = j.updateSchemaIfNeeded(ctx, tableReference, service); err != nil {
			return err
		}
	}
	if j.Rule.Dest.HasSplit() {
		if err = j.initTableSplit(ctx, service); err != nil {
			return errors.Wrapf(err, "failed to apply split schema optimization: %+v", j.Rule.Dest.Schema.Split)
		}
	}

	if tableReference != nil {
		if err = j.updateTableExpiryIfNeeded(ctx, service, tableReference); err != nil {
			return err
		}
	}

	j.Actions, err = j.buildActions()
	if err != nil {
		return errors.Wrapf(err, "failed to build actions")
	}

	if j.Rule.Dest.HasTemplate() {
		j.Load.Schema = nil
		j.Load.TimePartitioning = nil
		j.Load.Clustering = nil
		j.Load.RangePartitioning = nil
	}
	return nil
}

func (j *Job) updateTableExpiryIfNeeded(ctx context.Context, service bq.Service, tableReference *bigquery.TableReference) error {
	if j.Rule.Dest.Expiry == "" {
		return nil
	}
	destTable := base.EncodeTableReference(tableReference, false)
	expiry, err := toolbox.TimeAt(j.Rule.Dest.Expiry + " ahead in UTC")
	if err != nil {
		return errors.Wrapf(err, "invalid expiry expression: %v", j.Rule.Dest.Expiry)
	}
	if shared.IsInfoLoggingLevel() {
		shared.LogF("Setting %v to expire at: %v", destTable, expiry.Format(time.RFC3339))
	}
	table, err := service.Table(ctx, tableReference)
	if err != nil {
		return errors.Wrapf(err, "failed to get table %v for setting expiry: %v", destTable, j.Rule.Dest.Expiry)
	}
	table.ExpirationTime = expiry.Unix() * 1000
	_, err = service.Patch(ctx, &bq.PatchRequest{
		Table:         destTable,
		TemplateTable: table,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to set expiry: %v, on %v", destTable, j.Rule.Dest.Expiry)
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
