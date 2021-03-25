package bqtail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/bqtail/dispatch"
	"github.com/viant/bqtail/dispatch/contract"
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
	response.Lock()
	defer response.UnLock()
	data, _ := json.Marshal(response)
	fmt.Printf("%s\n", data)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
