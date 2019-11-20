package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const deleteSleepTime = 300

//Delete deletes supplied URLs
func (s *service) Delete(ctx context.Context, request *DeleteRequest) error {
	request.Init()
	err := request.Validate()
	if err != nil {
		return err
	}
	waitGroup := &sync.WaitGroup{}
	var errorChannel = make(chan error, 1)
	var hasError int32 = 0
	processed := map[string]bool{}

	for i := range request.URLs {
		if processed[request.URLs[i]] {
			continue
		}
		processed[request.URLs[i]] = true
		waitGroup.Add(1)

		if i%5 == 0 { //extra sleep to not exceed 15 req/sec
			time.Sleep(deleteSleepTime * time.Millisecond)
		}
		go func(URL string) {
			defer waitGroup.Done()
			if e := s.fs.Delete(ctx, URL); e != nil {
				if ok, err := s.fs.Exists(ctx, URL); !ok && err == nil {
					return
				}
				if atomic.CompareAndSwapInt32(&hasError, 0, 1) {
					errorChannel <- e
				}
			}
		}(request.URLs[i])
	}
	waitGroup.Wait()

	if atomic.LoadInt32(&hasError) == 1 {
		err = <-errorChannel
	}
	return err
}

//DeleteRequest delete request
type DeleteRequest struct {
	URLs      []string
	SourceURL string
}

//Init initialize request
func (r *DeleteRequest) Init() {
	if len(r.URLs) == 0 {
		r.URLs = make([]string, 0)
	}
	if r.SourceURL != "" {
		r.URLs = append(r.URLs, r.SourceURL)
	}
}

//Validate check if request is valid
func (r *DeleteRequest) Validate() error {
	if len(r.URLs) == 0 {
		return fmt.Errorf("urls was empty")
	}
	return nil
}
