package bq

import (
	"context"
	"github.com/viant/bqtail/base"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) createFromTemplate(ctx context.Context, template string, dest *bigquery.TableReference) (err error) {
	if template == "" {
		return err
	}
	tempRef, _ := base.NewTableReference(template)
	if table, err := s.Table(ctx, tempRef); err == nil {
		table.TableReference = dest
		err = s.CreateTableIfNotExist(ctx, table, true)
	}
	return err
}
