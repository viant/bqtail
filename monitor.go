package bqtail

import (
	"bqtail/base"
	"bqtail/mon"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
	"log"
	"net/http"
)

//Monitor cloud function entry point
func Monitor(w http.ResponseWriter, r *http.Request) {
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
	} else {
		if err := httpRequest.ParseForm(); err == nil {
			if len(httpRequest.Form) > 0 {
				request.IncludeDone = toolbox.AsBoolean(httpRequest.Form.Get("IncludeDone"))
				request.Recency = httpRequest.Form.Get("Recency")
				request.DestBucket = httpRequest.Form.Get("DestBucket")
				request.DestPath = httpRequest.Form.Get("DestPath")
			}
		}
	}

	if request.Recency == "" {
		request.Recency = "1hour"
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
