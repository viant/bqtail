package bqtail

import (
	"bqtail/dispatch"
	"bqtail/dispatch/contract"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

//BqDispatch BigQuery trigger background cloud function entry point
func BqDispatch(w http.ResponseWriter, r *http.Request) {
	_, err := handleDispatchEvent(context.Background())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleDispatchEvent(ctx context.Context) (*contract.Response, error) {
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return nil, err
	}
	response := service.Dispatch(ctx)
	if data, err := json.Marshal(response); err == nil {
		fmt.Printf("%v\n", string(data))
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}

	return response, nil
}
