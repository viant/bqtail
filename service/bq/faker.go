package bq

import (
	"github.com/viant/bqtail/base"
	"context"
	"github.com/pkg/errors"
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

//NewFakerWithTables creates a faker with tables
func NewFakerWithTables(tables map[string]*bigquery.Table) Service {
	return &faker{tables:tables}
}

