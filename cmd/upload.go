package cmd

import (
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/cmd/history"
	"github.com/viant/bqtail/cmd/tail"
	"github.com/viant/bqtail/cmd/uploader"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"sync/atomic"
	"time"
)

var minAgeUpload = 5 * time.Second

func (s *service) upload(ctx context.Context, destURL string, object storage.Object, uploadService uploader.Service, request *tail.Request, response *tail.Response) error {
	if object.IsDir() {
		objects, err := s.fs.List(ctx, object.URL())
		if err != nil {
			return err
		}
		eventsHistory, err := history.FromURL(ctx, request.HistoryPathURL(object.URL()), s.fs)
		if err != nil {
			return err
		}
		for i := range objects {
			if url.Equals(object.URL(), objects[i].URL()) {
				continue
			}
			if !objects[i].IsDir() {
				age := time.Now().Sub(objects[i].ModTime())
				if age < minAgeUpload && age >= 0 {
					continue
				}
				if !eventsHistory.Put(stage.NewSource(objects[i].URL(), objects[i].ModTime())) {
					continue
				}
				response.AddDataURL(objects[i].URL())
			}
			destURL := url.Join(destURL, objects[i].Name())
			if err = s.upload(ctx, destURL, objects[i], uploadService, request, response); err != nil {
				return err
			}
		}

		if len(eventsHistory.Events) > 0 {
			if err = eventsHistory.Persist(ctx, s.fs); err != nil {
				response.AddError(err)
			}
			response.AddHistoryURL(eventsHistory.URL)

		}
		return nil
	}

	if shared.IsDebugLoggingLevel() {
		shared.LogF("scheduling: %v\n", destURL)
	}
	uploadService.Schedule(uploader.NewRequest(object.URL(), destURL))
	return nil
}

//onUpload returns a callback function which is called per each uploader file
func (s *service) onUpload(ctx context.Context, response *tail.Response) func(URL string, err error) {
	return func(URL string, err error) {
		var object storage.Object
		if err == nil {
			atomic.AddInt32(&response.Info.Uplodaded, 1)
			if object, err = s.fs.Object(ctx, URL, option.NewObjectKind(true)); err == nil {
				err = s.emit(ctx, object, response)
			}
		}
		if err != nil {
			response.AddError(err)
			s.Stop()
		}
	}
}
