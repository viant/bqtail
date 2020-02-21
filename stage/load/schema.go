package load

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/shared"
	"google.golang.org/api/bigquery/v2"
)

func (j *Job) updateSchemaIfNeeded(ctx context.Context, tableReference *bigquery.TableReference, service bq.Service) error {
	var err error
	var table *bigquery.Table
	hasTransientTemplate := false
	transient := j.Rule.Dest.Transient

	if transient != nil {
		datasetRef := &bigquery.DatasetReference{ProjectId: j.ProjectID, DatasetId: transient.Dataset}

		if err := service.CreateDatasetIfNotExist(ctx, transient.Region, datasetRef); err != nil {
			return errors.Wrapf(err, "failed to create transient dataset: %v", transient.Dataset)
		}
		j.Load.WriteDisposition = shared.WriteDispositionTruncate
		if hasTransientTemplate = transient.Template != ""; hasTransientTemplate {
			transientTempRef, err := base.NewTableReference(transient.Template)
			if err != nil {
				return errors.Wrapf(err, "failed to create table from transient.Template: %v", transient.Template)
			}
			if table, err = service.Table(ctx, transientTempRef); err != nil {
				return errors.Wrapf(err, "failed to get template table: %v", transientTempRef)
			}
			j.TempSchema = table
		}
	}

	if j.Rule.Dest.Schema.Template != "" {
		templateRef, err := base.NewTableReference(j.Rule.Dest.Schema.Template)
		if err != nil {
			return errors.Wrapf(err, "invalid template: %v", j.Rule.Dest.Schema.Template)
		}
		table, err = service.Table(ctx, templateRef)
		if err != nil {
			return errors.Wrapf(err, "fail to get template table: %v", j.Rule.Dest.Schema.Template)
		}
		table.TableReference, _ = base.NewTableReference(j.DestTable)
		if err = service.CreateTableIfNotExist(ctx, table); err != nil {
			return errors.Wrapf(err, "failed to create table: %v", base.EncodeTableReference(tableReference, false))
		}
		j.DestSchema = table
	}

	if table == nil && !hasTransientTemplate {
		if table, err = service.Table(ctx, tableReference); err != nil {
			return errors.Wrapf(err, "failed to get table: %+v", tableReference)
		}
		j.DestSchema = table
	}

	if j.TempSchema != nil {
		j.updateSchema(j.TempSchema)
	} else if j.DestSchema != nil {
		j.updateSchema(j.DestSchema)
	}
	return nil
}

func (j *Job) updateSchema(table *bigquery.Table) {
	if table != nil {
		if j.Rule.Dest.Schema.Autodetect {
			j.Load.Schema = nil
			j.Load.Autodetect = j.Rule.Dest.Schema.Autodetect
			return
		}
		j.Load.Schema = table.Schema
		if table.TimePartitioning != nil {
			j.Load.TimePartitioning = table.TimePartitioning
			j.Load.TimePartitioning.RequirePartitionFilter = false
		}
		if table.RangePartitioning != nil {
			j.Load.RangePartitioning = table.RangePartitioning
		}
		if table.Clustering != nil {
			j.Load.Clustering = table.Clustering
		}
	}
}
