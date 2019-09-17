package slack

import (
	"bqtail/task"
	"context"
	"fmt"
)

//Run runs slack action
func (s *service) Run(ctx context.Context, request task.Request) error {
	switch req := request.(type) {
	case *NotifyRequest:
		return s.Notify(ctx, req)
	default:
		return fmt.Errorf("unsupported request type:%T", request)
	}
}
