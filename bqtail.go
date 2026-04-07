package bqtail

import (
	"context"
	"errors"
	"fmt"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail"
	"github.com/viant/bqtail/tail/contract"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/googleapis/google-cloudevents-go/cloud/storagedata"
)

func init() {
	functions.CloudEvent("BqTail", BqTail)
}

//BqTail storage trigger background cloud function entry point
func BqTail(ctx context.Context, e event.Event) error {
	var data storagedata.StorageObjectData
	if err := protojson.Unmarshal(e.Data(), &data); err != nil {
		return fmt.Errorf("failed to unmarshal event data: %w", err)
	}
	gsEvent := contract.GSEvent{
		Bucket: data.GetBucket(),
		Name:   data.GetName(),
	}
	request := &contract.Request{
		EventID:   e.ID(),
		SourceURL: gsEvent.URL(),
		Started:   e.Time(),
	}

	_, err := handleTailEvent(ctx, request)
	return err
}

func handleTailEvent(ctx context.Context, request *contract.Request) (*contract.Response, error) {
	service, err := tail.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Tail(ctx, request)
	shared.LogLn(response)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
