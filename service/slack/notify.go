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

//NotifyRequest represents a notify request
type NotifyRequest struct {
	Channels []string
	From     string
	Title    string
	Message  string
	Filename string
	Body     interface{}
	Secret   *base.Secret
	BodyType string
	OAuthToken
}

func (s *service) Notify(ctx context.Context, request *NotifyRequest) error {
	err := s.notify(ctx, request)
	if err != nil {
		err = errors.Wrapf(err, "failed to notify on slack: %v", request.Channels)
		fmt.Printf("%v\n", err)
	}
	return err
}


func (s *service) notify(ctx context.Context, request *NotifyRequest) error {
	err := request.Init(s.Region, s.projectID)
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		return err
	}
	if request.OAuthToken.Token == "" {
		if request.Secret == nil {
			request.Secret = s.defaultSecrets
		}
		err = s.Secret.Decode(ctx, s.Storage, request.Secret, &request.OAuthToken)
		if err != nil {
			return err
		}
	}
	client := slack.New(request.Token)
	return s.postMessage(ctx, client, request)
}

func (s *service) uploadFile(context context.Context, client *slack.Client, request *NotifyRequest) error {
	if request.Body == nil {
		return nil
	}
	body := ""
	switch value := request.Body.(type) {
	case []byte:
		body = string(value)
	case string:
		body = value
	default:
		data, err := json.MarshalIndent(value, "", "\t")
		if err != nil {
			err = errors.Wrapf(err, "failed to decode body: %v", value)
			return err
		}
		body = string(data)
	}
	if body == "" {
		return nil
	}

	fileType := "text"
	if json.Valid([]byte(body)) {
		fileType = "json"
	}
	uploadRequest := slack.FileUploadParameters{
		Filename: request.Filename,
		Title:    request.Title,
		Filetype: fileType,
		Content:  string(body),
		Channels: request.Channels,
	}
	_, err := client.UploadFile(uploadRequest)
	return err
}

func (s *service) postMessage(context context.Context, client *slack.Client, request *NotifyRequest) error {
	err := s.sendMessage(context, client, request)
	if err == nil {
		err = s.uploadFile(context, client, request)
	}
	return err
}

func (s *service) sendMessage(context context.Context, client *slack.Client, request *NotifyRequest) (err error) {
	if request.Message == "" {
		return nil
	}
	attachment := slack.Attachment{
		Text:       request.Message,
		AuthorName: request.From,
	}
	for _, channel := range request.Channels {
		if _, _, e := client.PostMessage(channel, slack.MsgOptionText(request.Title, false), slack.MsgOptionAttachments(attachment)); e != nil {
			err = e
		}
	}
	return err
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
	if r.Secret == nil && r.OAuthToken.Token == "" {
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
