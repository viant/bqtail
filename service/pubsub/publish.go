package pubsub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/viant/bqtail/task"
	"github.com/viant/toolbox"
	"google.golang.org/api/pubsub/v1"
)

//PushRequest represents push message request
type PushRequest struct {
	ProjectID  string
	Topic      string
	Data       []byte
	Message    interface{}
	Attributes map[string]interface{}
}

//PushResponse represents push response message
type PushResponse struct {
	MessageIDs []string
}

//Publish publishes data to message bus
func (s *service) Publish(ctx context.Context, request *PushRequest, action *task.Action) (task.Response, error) {
	if request.ProjectID == "" {
		request.ProjectID = s.ProjectID
	}
	response := &PushResponse{MessageIDs: make([]string, 0)}
	return response, s.publish(ctx, request, response)
}

func (s *service) publish(ctx context.Context, request *PushRequest, response *PushResponse) error {
	message := ""
	if request.Message != nil {
		text, ok := request.Message.(string)
		if ok {
			message = text
		} else {
			aMap := toolbox.AsMap(request.Message)
			if JSON, err := json.Marshal(aMap); err == nil {
				message = string(JSON)
			}
		}
	}
	if len(request.Data) == 0 {
		request.Data = []byte(message)
	}

	publishRequest := &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{
			{
				Data: base64.StdEncoding.EncodeToString(request.Data),
			},
		},
	}
	if len(request.Attributes) > 0 {
		publishRequest.Messages[0].Attributes = make(map[string]string)
		for k, v := range request.Attributes {
			publishRequest.Messages[0].Attributes[k] = fmt.Sprintf("%s", v)
		}
	}
	topic := s.topicInProject(request)
	publishCall := pubsub.NewProjectsService(s.Service).Topics.Publish(topic, publishRequest)

	publishCall.Context(ctx)
	callResponse, err := publishCall.Do()
	if err == nil {
		response.MessageIDs = callResponse.MessageIds
	}
	return err
}
