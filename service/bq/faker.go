package bq

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"google.golang.org/api/bigquery/v2"
)

type faker struct {
	Service
	tables map[string]*bigquery.Table
}

func (f *faker) Table(ctx context.Context, reference *bigquery.TableReference) (*bigquery.Table, error) {
	key := base.EncodeTableReference(reference, false)
	if len(f.tables) == 0 || f.tables[key] == nil {
		return nil, errors.Errorf("not found table: %v", key)
	}
	return f.tables[key], nil
}

func (f *faker) CreateDatasetIfNotExist(ctx context.Context, region string, dataset *bigquery.DatasetReference) error {
	return nil
}

func (f *faker) CreateTableIfNotExist(ctx context.Context, table *bigquery.Table) error {
	return nil
}

//NewFakerWithTables creates a faker with tables
func NewFakerWithTables(tables map[string]*bigquery.Table) Service {
	return &faker{tables: tables}
}
