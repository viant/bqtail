package slack

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/stage/activity"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nlopes/slack"
	"github.com/pkg/errors"
	"strings"
)

//NotifyRequest represents a notify request
type NotifyRequest struct {
	Root        *activity.Meta
	Channels    []string
	From        string
	Title       string
	Message     string
	Filename    string
	Error       string
	Body        interface{}
	Response    interface{}
	Source      string
	Credentials *base.Secret
	BodyType    string
	OAuthToken
}

func (s *service) Notify(ctx context.Context, request *NotifyRequest) error {
	err := s.notify(ctx, request)
	if err != nil {
		JSON, _ := json.Marshal(request)
		err = errors.Wrapf(err, "failed to notify on slack: %v", JSON)
	}
	return err
}

func (s *service) notify(ctx context.Context, request *NotifyRequest) error {
	if request.Credentials == nil {
		request.Credentials = s.defaultSecrets
	}
	err := request.Init(s.Region, s.projectID)
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		return err
	}
	if request.OAuthToken.Token == "" {
		err = s.Secret.Decode(ctx, s.Storage, request.Credentials, &request.OAuthToken)
		if err != nil {
			return errors.Wrapf(err, "failed to decode token: from %v %v", request.Credentials.Key, request.Credentials.URL)
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
		if body == "$Response" {
			request.Body = request.Response
		}
	default:
		data, err := json.MarshalIndent(request.Body, "", "\t")
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
	if r.Credentials != nil {
		if strings.Count(r.Credentials.Key, "/") == 1 {
			pair := strings.Split(r.Credentials.Key, "/")
			ring := strings.TrimSpace(pair[0])
			key := strings.TrimSpace(pair[1])
			r.Credentials.Key = fmt.Sprintf("projects/%v/locations/%v/keyRings/%v/cryptoKeys/%v", projectID, location, ring, key)
		}
	}
	if r.Message != "" {
		r.Message = strings.Replace(r.Message, "$Source", r.Source, 1)
		r.Message = strings.Replace(r.Message, "$Error", r.Error, 1)
	}

	if r.Title != "" {
		r.Title = strings.Replace(r.Title, "$Source", r.Source, 1)
	}
	if r.Filename != "" {
		r.Filename = strings.Replace(r.Filename, "$Source", r.Source, 1)

	}
	return nil
}

//Validate checks if request is valid
func (r *NotifyRequest) Validate() error {
	if r.Credentials == nil && r.OAuthToken.Token == "" {
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
