package bqtail

import (
	"bqtail/tail"
	"bqtail/tail/contract"
	"cloud.google.com/go/functions/metadata"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

//BqTail storage trigger background cloud function entry point
func BqTail(ctx context.Context, event contract.GSEvent) (err error) {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	request := &contract.Request{
		EventID:   meta.EventID,
		SourceURL: event.URL(),
	}
	_, err = handleTailEvent(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func handleTailEvent(ctx context.Context, request *contract.Request) (*contract.Response, error) {
	service, err := tail.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Tail(ctx, request)
	if data, err := json.Marshal(response); err == nil {
		fmt.Printf("%v\n", string(data))
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
