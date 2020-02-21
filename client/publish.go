package client

import (
	"context"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/bqtail/tail/contract"
	"math/rand"
	"sync/atomic"
	"time"
)

func (s *service) publish(ctx context.Context, object storage.Object, response *TailResponse) error {
	if object.IsDir() {
		objects, err := s.fs.List(ctx, object.URL())
		if err != nil {
			return err
		}
		for _, object := range objects {
			if err := s.publish(ctx, object, response); err != nil {
				return err
			}
		}
		return nil
	}
	request := &contract.Request{
		EventID:   fmt.Sprintf("%v", nextEventID()),
		SourceURL: object.URL(),
	}
	atomic.AddInt32(&response.Published, 1)
	atomic.AddInt32(&response.pending, 1)
	s.requestChan <- request
	return nil
}

func nextEventID() uint64 {
	rand.Seed((time.Now().UTC().UnixNano()))
	return rand.Uint64()
}
