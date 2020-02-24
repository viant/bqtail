package cmd

import (
	"context"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/cmd/tail"
	"github.com/viant/bqtail/tail/contract"
	"math/rand"
	"sync/atomic"
	"time"
)

func (s *service) emit(ctx context.Context, object storage.Object, response *tail.Response) error {
	if object.IsDir() {
		objects, err := s.fs.List(ctx, object.URL())
		if err != nil {
			return err
		}
		for i := range objects {
			if url.Equals(object.URL(), objects[i].URL()) {
				continue
			}
			if err := s.emit(ctx, objects[i], response); err != nil {
				return err
			}
		}
		return nil
	}
	request := &contract.Request{
		EventID:   fmt.Sprintf("%v", nextEventID()),
		SourceURL: object.URL(),
	}
	atomic.AddInt32(&response.Info.Published, 1)
	response.IncrementPending(1)
	s.requestChan <- request
	return nil
}

func nextEventID() uint64 {
	rand.Seed((time.Now().UTC().UnixNano()))
	return rand.Uint64()
}
