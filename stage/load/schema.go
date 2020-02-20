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

	transient := j.Rule.Dest.Transient
	if transient != nil {
		j.Load.WriteDisposition = shared.WriteDispositionTruncate
		if transient.Template != "" {
			templateReference, err := base.NewTableReference(transient.Template)
			if err != nil {
				return errors.Wrapf(err, "failed to create table from transient.Template: %v", transient.Template)
			}
			if table, err = service.Table(ctx, templateReference); err != nil {
				return errors.Wrapf(err, "failed to get template table: %v", templateReference)
			}
			j.TempSchema = table.Schema
			j.updateSchema(table)
		}
	}

	if j.Rule.Dest.Schema.Table != nil {
		j.DestSchema = j.Rule.Dest.Schema.Table
		return nil
	}
	if table == nil && transient != nil {
		if table, err = service.Table(ctx, tableReference); err != nil {
			return errors.Wrapf(err, "failed to get table: %+v", tableReference)
		}
	}
	if table != nil {
		j.DestSchema = table.Schema
	}

	if transient != nil && transient.Template == "" {
		j.updateSchema(table)
	}
	return nil
}

func (j *Job) updateSchema(table *bigquery.Table) {
	if table != nil {
		j.Load.Schema = table.Schema
		if j.Load.Schema == nil && j.Rule.Dest.Schema.Autodetect {
			j.Load.Autodetect = j.Rule.Dest.Schema.Autodetect
		}
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
