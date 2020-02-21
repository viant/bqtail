package client

import (
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/client/tail"
	"github.com/viant/bqtail/client/uploader"
	"sync/atomic"
)



func (s *service) upload(ctx context.Context, baseDestURL string, object storage.Object, uploadService uploader.Service)  error {
	if object.IsDir() {
		objects, err := s.fs.List(ctx, object.URL())
		if err != nil {
			return err
		}

		for i := range objects {
			if url.IsSchemeEquals(object.URL(), objects[i].URL()) {
				continue
			}
			destURL := url.Join(baseDestURL, objects[i].URL())
			if objects[i].IsDir() {
				if err = s.upload(ctx, destURL, objects[i], uploadService);err != nil {
					return err
				}
				continue
			}
			uploadService.Schedule(uploader.NewRequest(objects[i].URL(), destURL))
		}
	}
	return nil
}

//onUpload returns a callback function which is called per each uploader file
func (s *service) onUpload(ctx context.Context, response *tail.Response) func (URL string, err error) {
	return func(URL string, err error) {
		var object storage.Object
		if err == nil {
			atomic.AddInt32(&response.Uplodaded, 1)
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
