package batch

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/task"
)

//Run handles fs request
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	switch req := request.ServiceRequest().(type) {
	case *GroupRequest:
		return nil, s.OnDone(ctx, req, request)
	}
	return nil, fmt.Errorf("unsupported request type:%T", request)
}
