package load

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"google.golang.org/api/bigquery/v2"
)

//updateSchemaIfNeeded, determine load job table schema fields
// or in case of templates create/patch temp or dest table respectively
func (j *Job) updateSchemaIfNeeded(ctx context.Context, tableReference *bigquery.TableReference, service bq.Service) error {
	var err error
	var table *bigquery.Table
	hasTransientTemplate := false
	transient := j.Rule.Dest.Transient

	if j.Rule.Dest.Schema.Template != "" {
		if table, err = j.applyDestTemplate(table, service, ctx, tableReference); err != nil {
			return err
		}
	}

	if j.Rule.Dest.Schema.Autodetect {
		j.Load.Schema = nil
		j.Load.Autodetect = true
		return nil
	}

	if transient != nil {
		datasetRef := &bigquery.DatasetReference{ProjectId: j.ProjectID, DatasetId: transient.Dataset}
		if err := service.CreateDatasetIfNotExist(ctx, transient.Region, datasetRef); err != nil {
			return errors.Wrapf(err, "failed to create transient dataset: %v", transient.Dataset)
		}
		if hasTransientTemplate = transient.Template != ""; hasTransientTemplate {
			transientTempRef, _ := base.NewTableReference(transient.Template)
			if table, err = service.Table(ctx, transientTempRef); err != nil {
				return errors.Wrapf(err, "failed to get transient.template table: %v", base.EncodeTableReference(transientTempRef, false))
			}
		}
		if table != nil {
			j.TempSchema = table
			tableRef, _ := base.NewTableReference(j.TempTable)
			if j.Rule.Dest.HasSplit() {
				tableRef, _ = base.NewTableReference(j.SplitTable())
			}
			table.TableReference = tableRef
			resetExpiryTime(table)
			if err = service.CreateTableIfNotExist(ctx, table, false); err != nil {
				return errors.Wrapf(err, "failed to create transient table: %v", base.EncodeTableReference(tableReference, false))
			}
		}
	}

	if table == nil && !hasTransientTemplate {
		if table, err = service.Table(ctx, tableReference); err != nil {
			return errors.Wrapf(err, "failed to get table: %v", base.EncodeTableReference(tableReference, false))
		}
		j.DestSchema = table
	}

	if j.TempSchema != nil {
		if err = j.updateSchema(j.TempSchema); err != nil {
			return err
		}
	} else if j.DestSchema != nil {
		err = j.updateSchema(j.DestSchema)
	}
	return err
}

func (j *Job) applyDestTemplate(table *bigquery.Table, service bq.Service, ctx context.Context, tableReference *bigquery.TableReference) (*bigquery.Table, error) {
	templateRef, err := base.NewTableReference(j.Rule.Dest.Schema.Template)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid template: %v", j.Rule.Dest.Schema.Template)
	}
	table, err = service.Table(ctx, templateRef)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to get template table: %v", j.Rule.Dest.Schema.Template)
	}
	table.TableReference, _ = base.NewTableReference(j.DestTable)
	resetExpiryTime(table)
	if err = service.CreateTableIfNotExist(ctx, table, true); err != nil {
		return nil, errors.Wrapf(err, "failed to create table: %v", base.EncodeTableReference(tableReference, false))
	}
	j.DestSchema = table
	return table, nil
}

func resetExpiryTime(table *bigquery.Table) {
	if table.ExpirationTime > 0 {
		table.ExpirationTime = 0
	}
}

func (j *Job) updateSchema(table *bigquery.Table) error {
	if table != nil {
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
	return nil
}
