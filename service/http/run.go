package http

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/task"
)

//Run runs slack action
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	switch req := request.ServiceRequest().(type) {
	case *CallRequest:
		return s.Call(ctx, req)
	}
	return nil, fmt.Errorf("unsupported request type:%T", request)
}
