package slack

import (
	"bqtail/base"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nlopes/slack"
	"github.com/pkg/errors"
	"strings"
)

func (s *service) Notify(ctx context.Context, request *NotifyRequest) error {
	err := request.Init(s.Region, s.projectID)
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		return err
	}
	if request.OAuthToken.Token == "" {
		err = s.Secret.Decode(ctx, s.Storage, request.Secret, &request.OAuthToken)
		if err != nil {
			return err
		}
	}
	client := slack.New(request.Token)
	return s.postMessage(ctx, client, request)
}

func (s *service) postMessage(context context.Context, client *slack.Client, request *NotifyRequest) error {
	body, err := getBody(request)
	if err != nil {
		return nil
	}
	attachment := slack.Attachment{
		Text:       request.Message,
		AuthorName: request.From,
		MarkdownIn: []string{"json"},
	}
	if body != nil {
		attachment.Fields = []slack.AttachmentField{
			{
				Value: string(body),
			},
		}
	}
	for _, channel := range request.Channels {
		_, _, err = client.PostMessage(channel, slack.MsgOptionText(request.Title, false), slack.MsgOptionAttachments(attachment))
	}
	return err
}

func getBody(request *NotifyRequest) ([]byte, error) {
	if request.Body != nil {
		return json.Marshal(request.Body)
	}
	return nil, nil
}

//NotifyRequest represents a notify request
type NotifyRequest struct {
	Channels []string
	From     string
	Title    string
	Message  string
	Body     interface{}
	Secret   *base.Secret
	BodyType string
	OAuthToken
}

//Init initializes request
func (r *NotifyRequest) Init(location, projectID string) error {
	if r.Secret != nil {
		if strings.Count(r.Secret.Key, "/") == 1 {
			pair := strings.Split(r.Secret.Key, "/")
			ring := strings.TrimSpace(pair[0])
			key := strings.TrimSpace(pair[1])
			r.Secret.Key = fmt.Sprintf("projects/%v/locations/%v/keyRings/%v/cryptoKeys/%v", projectID, location, ring, key)
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *NotifyRequest) Validate() error {
	if r.Secret == nil {
		return errors.New("secret was empty")
	}
	if len(r.Channels) == 0 {
		return errors.New("channels was empty")
	}
	if r.Title == "" {
		return errors.New("title was empty")
	}
	return nil
}
