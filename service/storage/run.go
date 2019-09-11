package storage

import (
	"context"
	"fmt"
	"bqtail/task"
)


func (s *service) Run(ctx context.Context, request task.Request) error {
	switch req := request.(type) {
	case *DeleteRequest:
		return s.Delete(ctx, req)
	case *MoveRequest:
		return s.Move(ctx, req)
	default:
		return fmt.Errorf("unsupported request type:%T", request)
	}
}

