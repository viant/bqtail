package slack

import (
	"bqtail/task"
	"context"
	"github.com/pkg/errors"
)

//Run runs slack action
func (s *service) Run(ctx context.Context, request task.Request) (task.Response, error) {
	switch req := request.(type) {
	case *NotifyRequest:
		return nil, s.Notify(ctx, req)
	}
	return nil, errors.Errorf("unsupported request type:%T", request)

}
