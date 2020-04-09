package cmd

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
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
	"path"

	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (s *service) Load(ctx context.Context, request *tail.Request) (*tail.Response, error) {
	if request.Bucket == "" {
		if url.Scheme(request.SourceURL, file.Scheme) == "gs" {
			request.Bucket = url.Host(request.SourceURL)
		}
	}
	request.Init(s.config)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rule, err := s.loadRule(ctx, request.RuleURL)
	if err != nil {
		return nil, err
	}
	s.reportSettings(request, s.config)
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
	if len(response.Errors) == 0 {
		if err := s.updateHistory(ctx, response); err != nil {
			response.AddError(err)
		}
	}
}

func (s *service) scanFiles(ctx context.Context, waitGroup *sync.WaitGroup, object storage.Object, rule *config.Rule, request *tail.Request, response *tail.Response) {
	defer waitGroup.Done()
	isGCS := url.Scheme(object.URL(), "") == gs.Scheme

	dataPrefix := prefix.Extract(rule)

	destURL := fmt.Sprintf("%v://%v/%v", gs.Scheme, request.Bucket, strings.Trim(dataPrefix, "/"))
	if isGCS {
		if rule.HasMatch(object.URL()) {
			if err := s.emit(ctx, object, response); err != nil {
				response.AddError(err)
				s.Stop()
			}
			return
		}
		if url.Equals(object.URL(), destURL) { //transient bucket is the same as source URL, when rule matched data,
			//no need to upload, just emit events, in that case original modification time will be used for batching
			matched := 0
			if objects, err := s.fs.List(ctx, object.URL()); err == nil {
				for _, object := range objects {
					if rule.HasMatch(object.URL()) {
						matched++
						if err := s.emit(ctx, object, response); err != nil {
							response.AddError(err)
						}
					}
				}
			}
			if matched > 0 {
				return
			}
		}
	}

	if rule.HasMatch(object.URL()) {
		sourcePath := url.Path(object.URL())
		dataPrefix, _ = path.Split(sourcePath)
		destURL = fmt.Sprintf("%v://%v/%v", gs.Scheme, request.Bucket, strings.Trim(dataPrefix, "/"))
	}

	uploadService := uploader.New(ctx, s.fs, s.onUpload(ctx, response), processingRoutines)
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
	var index = make(map[string]bool)
	for _, URL := range response.DataURLs() {
		index[URL] = true
	}
	for _, URL := range historyURLs {
		events, err := history.FromURL(ctx, URL, s.fs)
		if err != nil {
			return err
		}
		for i, event := range events.Events {
			if index[event.URL] {
				events.Events[i].Status = shared.StatusOK
			}
		}
		err = events.Persist(ctx, s.fs)
		if err != nil {
			return err
		}
	}
	return nil
}
