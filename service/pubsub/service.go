package pubsub

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/option"
	"google.golang.org/api/pubsub/v1"
	"strings"
)

//Service represents big query service
type Service interface {
	task.Service

	Publish(ctx context.Context, request *PushRequest, action *task.Action) (task.Response, error)
}

type service struct {
	*pubsub.Service
	ProjectID string
}

func (s *service) topicInProject(request *PushRequest) string {
	if strings.Count(request.Topic, "/") > 0 {
		return request.Topic
	}
	return fmt.Sprintf("projects/%s/topics/%s", request.ProjectID, request.Topic)
}

//New creates a service
func New(ctx context.Context, projectID string, options ...option.ClientOption) (Service, error) {
	srv, err := pubsub.NewService(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &service{
		Service:   srv,
		ProjectID: projectID,
	}, nil
}
