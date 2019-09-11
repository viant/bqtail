package bqtail

import (
	"cloud.google.com/go/functions/metadata"
	"context"
	"errors"
	"bqtail/base"
	"bqtail/tail"
	"bqtail/tail/contract"
	"log"
)



//BqTailFn storage trigger background cloud function entry point
func BqTailFn(ctx context.Context, event contract.GSEvent) (err error) {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	request := &contract.Request{
		EventID:   meta.EventID,
		SourceURL: event.URL(),
	}
	_, err = handleTailEvent(ctx, request)
	return err
}


func handleTailEvent(ctx context.Context, request *contract.Request) (*contract.Response, error) {
	service, err := tail.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Tail(ctx, request)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	if response.Status != base.StatusOK {
		log.Printf("Status: %v, Error: %v", response.Status, response.Error)
	} else {
		log.Printf("Status: %v, Time: %v, Matched: %v, Batched: %v, job: %v", response.Status, response.TimeTaken, response.Matched, response.Batched, response.JobRef)
	}
	return response, nil
}

