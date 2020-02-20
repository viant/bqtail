package pubsub

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/task"
)

//Run run request
func (s *service) Run(ctx context.Context, request *task.Action) (task.Response, error) {
	var response task.Response
	var err error
	serviceRequest := request.ServiceRequest()
	switch req := serviceRequest.(type) {
	case *PushRequest:
		response, err = s.Publish(ctx, req, request)
	default:
		return nil, errors.Errorf("unsupported request type:%T", request)
	}
	if err != nil {
		return nil, err
	}
	return response, nil
}
