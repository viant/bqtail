package cmd

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/cmd/history"
	"github.com/viant/bqtail/cmd/prefix"
	"github.com/viant/bqtail/cmd/tail"
	"github.com/viant/bqtail/cmd/uploader"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/contract"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (s *service) Load(ctx context.Context, request *tail.Request) (*tail.Response, error) {
	request.Init(s.config)

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
	response := tail.NewResponse()
	ctx, cancel := context.WithCancel(ctx)
	go s.tailInBackground(ctx, cancel)

	waitGroup := &sync.WaitGroup{}
	go s.handleResponse(ctx, response)
	for atomic.LoadInt32(&s.stopped) == 0 {
		s.loadDatafiles(waitGroup, ctx, object, rule, request, response)
		if !request.Stream {
			break
		}

		if response.Info.Uplodaded > 0 {
			shared.LogLn(response)
			response = tail.NewResponse()
		} else {
			time.Sleep(time.Second)
		}
	}

	s.Stop()
	return response, err
}

func (s *service) loadDatafiles(waitGroup *sync.WaitGroup, ctx context.Context, object storage.Object, rule *config.Rule, request *tail.Request, response *tail.Response) {
	waitGroup.Add(1)
	go s.scanFiles(ctx, waitGroup, object, rule, request, response)
	waitGroup.Wait()

	for atomic.LoadInt32(&s.stopped) == 0 && response.Pending() > 0 {
		time.Sleep(2 * time.Second)
		shared.LogProgress()
	}
	if err := s.updateHistory(ctx, response); err != nil {
		response.AddError(err)
	}
}

func (s *service) scanFiles(ctx context.Context, waitGroup *sync.WaitGroup, object storage.Object, rule *config.Rule, request *tail.Request, response *tail.Response) {
	defer waitGroup.Done()
	if rule.HasMatch(object.URL()) {
		if err := s.emit(ctx, object, response); err != nil {
			response.AddError(err)
			s.Stop()
		}
		return
	}
	uploadService := uploader.New(ctx, s.fs, s.onUpload(ctx, response), processingRoutines)
	dataPrefix := prefix.Extract(rule)
	destURL := fmt.Sprintf("%v://%v/%v", gs.Scheme, request.Bucket, strings.Trim(dataPrefix, "/"))
	if !object.IsDir() {
		destURL = url.Join(destURL, object.Name())
	}

	s.upload(ctx, destURL, object, uploadService, request, response)
	uploadService.Wait()
}

func (s *service) handleResponse(ctx context.Context, response *tail.Response) {
	for {
		select {
		case <-s.stopChan:
			return
		case resp := <-s.responseChan:
			if resp.Error != "" {
				s.Stop()
				response.AddError(errors.New(resp.Error))
				return
			}

			if resp.Batched {
				if resp.BatchRunner {
					processed := int32(len(resp.Window.URIs))
					response.IncrementPending(-processed)
					atomic.AddInt32(&response.Info.Loaded, processed)
					atomic.AddInt32(&response.Info.Batched, 1)
				}
			} else {
				response.IncrementPending(-1)
				if resp.Matched {
					atomic.AddInt32(&response.Info.Loaded, 1)
				} else {
					atomic.AddInt32(&response.Info.NoMatched, -1)
				}
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
			go func(req *contract.Request) {
				resp := s.tailService.Tail(ctx, req)
				s.responseChan <- resp
			}(req)
		}
	}
}

func (s *service) updateHistory(ctx context.Context, response *tail.Response) error {
	historyURLs := response.HistoryURLs()
	if len(historyURLs) == 0 {
		return nil
	}

	for _, URL := range historyURLs {
		events, err := history.FromURL(ctx, URL, s.fs)
		if err != nil {
			return err
		}
		events.Status = shared.StatusOK
		err = events.Persist(ctx, s.fs)
		if err != nil {
			return err
		}
	}
	return nil
}
