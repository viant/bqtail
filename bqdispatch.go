package bqtail

import (
	"bqtail/dispatch"
	"bqtail/dispatch/contract"
	"cloud.google.com/go/functions/metadata"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
)

//BqDispatch BigQuery trigger background cloud function entry point
func BqDispatch(ctx context.Context, event interface{}) (err error) {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	request := newRequest(meta)
	_, err = handleDispatchEvent(ctx, request)
	if err != nil {
		log.Printf("-ERROR: %v", err)
		//if base.IsPermissionDenied(err) {
		//
		//	return nil
		//}
	}
	return nil
}

func newRequest(meta *metadata.Metadata) *contract.Request {
	resourceParts := strings.Split(meta.Resource.Name, "/")
	return &contract.Request{
		EventID:   meta.EventID,
		ProjectID: resourceParts[1],
		JobID:     resourceParts[len(resourceParts)-1],
	}
}

func handleDispatchEvent(ctx context.Context, request *contract.Request) (*contract.Response, error) {
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Dispatch(ctx, request)
	if data, err := json.Marshal(response); err == nil {
		fmt.Printf("%v\n", string(data))
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}

	return response, nil
}
