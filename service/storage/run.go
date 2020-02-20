package storage

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/task"
)

//Run handles fs request
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	switch req := request.ServiceRequest().(type) {
	case *DeleteRequest:
		return nil, s.Delete(ctx, req)
	case *MoveRequest:
		return nil, s.Move(ctx, req)
	}
	return nil, errors.Errorf("unsupported request type:%T", request)
}
