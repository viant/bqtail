package bqtail

import (
	"bqtail/model"
	"cloud.google.com/go/functions/metadata"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"io"
	"log"
	"net/http"
)

//BQTailFn background cloud function entry point
func BQTailFn(ctx context.Context, event model.GSEvent) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
	}()
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return err
	}
	request := &Request{
		EventID:   meta.EventID,
		SourceURL: event.URL(),
	}
	_, err = handleEvent(request)
	return err
}

//HTTPBQTailFn HTTP cloud function entry point
func HTTPBQTailFn(writer http.ResponseWriter, httpRequest *http.Request) {
	request, err := newRequest(httpRequest.Body)
	if err == nil {
		var response *Response
		if response, err = handleEvent(request); response != nil {
			err = json.NewEncoder(writer).Encode(response)
		}
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func newRequest(reader io.Reader) (*Request, error) {
	decoder := json.NewDecoder(reader)
	request := &Request{}
	return request, decoder.Decode(&request)
}

func handleEvent(request *Request) (*Response, error) {
	service, err := GetService()
	if err != nil {
		return nil, err
	}
	toolbox.Dump(request)
	response := service.Tail(request)
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	if response.Status != StatusOK {
		log.Printf("Status: %v, Error: %v", response.Status, response.Error)
	} else {
		log.Printf("Status: %v, Time: %v, Mode: %v, Jobs: %v", response.Status, response.TimeTaken, response.Mode, len(response.JobRefs))
	}
	return response, nil
}
