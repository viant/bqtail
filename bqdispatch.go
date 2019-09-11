package bqtail

import (
	"cloud.google.com/go/functions/metadata"
	"context"
	"errors"
	"bqtail/base"
	"bqtail/dispatch"
	"bqtail/dispatch/contract"
	"log"
	"strings"
)



//BqDispatchFn BigQuery trigger background cloud function entry point
func BqDispatchFn(ctx context.Context, event interface{}) (err error) {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	request := newRequest(meta)
	_, err = handleDispatchEvent(ctx, request)
	return err
}


func newRequest(meta *metadata.Metadata) *contract.Request {
	resourceParts := strings.Split(meta.Resource.Name, "/")
	return &contract.Request{
		EventID:meta.EventID,
		ProjectID:resourceParts[1],
		JobID:resourceParts[len(resourceParts)-1],
	}
}

func handleDispatchEvent(ctx context.Context, request *contract.Request) (*contract.Response, error) {
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Dispatch(ctx, request)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	if response.Status != base.StatusOK {
		log.Printf("Status: %v, Error: %v", response.Status, response.Error)
	} else {
		jobID :=""
		if response.JobRef != nil {
			jobID = response.JobRef.JobId
		}
		log.Printf("Status: %v, Time: %v, Matched: %v, Job: %v", response.Status, response.TimeTaken, response.Matched, jobID)
	}
	return response, nil
}

