package bq

import (
	"context"
	"github.com/viant/bqtail/base"
)

type TableExistsRequest struct {
	Table string
}

//TableExists returns true if table exists
func (s *service) TableExists(ctx context.Context, request *TableExistsRequest) (bool, error) {
	tableRef, err := base.NewTableReference(request.Table)
	if err != nil {
		return false, err
	}
	table, err := s.Table(ctx, tableRef)
	if base.IsNotFoundError(err) {
		err = nil
	}
	return table != nil, err
}
