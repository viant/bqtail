package client

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/storage"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/client/prefix"
	"github.com/viant/bqtail/client/tail"
	"github.com/viant/bqtail/client/uploader"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/toolbox"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)


func (s *service) Tail(ctx context.Context, request *tail.Request) (*tail.Response, error) {
	request.Init(s.config)
	toolbox.Dump(request)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rule, err := s.loadRule(ctx, request.RuleURL)
	if err != nil {
		return nil, err
	}
	s.reportRule(rule)
	object, err := s.fs.Object(ctx, request.SourceURL)
	if err != nil {
		return nil, errors.Wrapf(err, "source location not found: %v", request.SourceURL)
	}

	response := &tail.Response{}
	ctx, cancel := context.WithCancel(ctx)
	go s.tailInBackground(ctx, cancel)
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	go s.tail(ctx, waitGroup, object, rule, request, response)
	go s.handleResponse(ctx, response)
	waitGroup.Wait()

	for atomic.LoadInt32(&s.stopped) == 0 && response.Pending() > 0 {
		time.Sleep(2 * time.Second)
		shared.LogProgress()
	}
	s.Stop()
	return response, err
}


func (s *service) tail(ctx context.Context, waitGroup *sync.WaitGroup, object storage.Object, rule *config.Rule, request * tail.Request, response *tail.Response) {
	defer waitGroup.Done()
	if rule.HasMatch(object.URL()) {
		if err := s.emit(ctx, object, response); err != nil {
			response.AddError(err)
			s.Stop()
		}
		return
	}
	uploadService := uploader.New(ctx, s.fs,s.onUpload(ctx, response), processingRoutines)
	dataPrefix := prefix.Extract(rule)
	destURL := fmt.Sprintf("%v://%v/%v", gs.Scheme, request.Bucket, strings.Trim(dataPrefix, "/"))
	s.upload(ctx, destURL, object, uploadService)
	uploadService.Wait()


}





func (s *service) handleResponse(ctx context.Context, response *tail.Response) {
	for {
		select {
		case <-s.stopChan:
			return
		case resp := <-s.responseChan:
			toolbox.DumpIndent(resp, true)
			if resp.Error != "" {
				s.Stop()
				response.AddError(errors.New(resp.Error))
				return
			}
			if resp.BatchRunner {
				atomic.AddInt32(&response.Batches, 1)
				if window, ok := resp.Window.(batch.Info); ok {
					atomic.AddInt32(&response.Loaded, int32(len(window.URIs)))
					response.IncrementPending(-int32(len(window.URIs)))
				}
			}
			if resp.Window == nil && resp.Matched {
				response.IncrementPending(-1)
				atomic.AddInt32(&response.Loaded, 1)
			}
			if !resp.Matched {
				atomic.AddInt32(&response.NoMatched, -1)
			}
		}
	}
}


func (s *service) tailInBackground(ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
		case <-s.stopChan:
			cancel()
			return
		case req := <-s.requestChan:
			resp := s.tailService.Tail(ctx, req)
			s.responseChan <- resp
		}
	}
}
