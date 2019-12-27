package http

import (
	"bqtail/task"
	"context"
	"fmt"
)

//Run runs slack action
func (s *service) Run(ctx context.Context, request task.Request) (task.Response, error) {
	switch req := request.(type) {
	case *CallRequest:
		return s.Call(ctx, req)
	}
	return nil, fmt.Errorf("unsupported request type:%T", request)
}
