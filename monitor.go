package bqtail

import (
	"bqtail/base"
	"bqtail/mon"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"net/http"
)

//BqTailMonitor cloud function entry point
func BqTailMonitor(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > 0 {
		defer func() {
			_ = r.Body.Close()
		}()
	}
	err := checkBqTailPerformance(w, r)
	if err != nil {
		log.Print(err)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func checkBqTailPerformance(writer http.ResponseWriter, httpRequest *http.Request) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	request := &mon.Request{}
	if httpRequest.ContentLength > 0 {
		if err = json.NewDecoder(httpRequest.Body).Decode(&request); err != nil {
			return errors.Wrapf(err, "failed to decode %T", request)
		}
	}
	ctx := context.Background()
	service, err := mon.Singleton(ctx, base.ConfigEnvKey)
	if err != nil {
		return err
	}
	response := service.Check(ctx, request)
	writer.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(writer).Encode(response); err != nil {
		return err
	}
	return err
}
