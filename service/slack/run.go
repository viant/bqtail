package slack

import (
	"github.com/viant/bqtail/task"
	"context"
	"github.com/pkg/errors"
)

//Run runs slack action
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	switch req := request.ServiceRequest().(type) {
	case *NotifyRequest:
		return nil, s.Notify(ctx, req)
	}
	return nil, errors.Errorf("unsupported request type:%T", request)

}
