package client

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v2"
	"sync"
	"sync/atomic"
	"time"
)

type TailRequest struct {
	Force     bool
	RuleURL   string
	Build     *BuildRuleRequest
	SourceURL string
}

func (t TailRequest) Validate() error {
	if t.SourceURL == "" {
		return errors.New("sourceURL was empty")
	}
	return nil
}

type TailResponse struct {
	Published int32
	Batches   int32
	NoMatched int32
	Loaded    int32
	pending   int32
	Errors    []string
	mux       sync.Mutex
}

func (r *TailResponse) AddError(err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Errors) == 0 {
		r.Errors = make([]string, 0)
	}
	r.Errors = append(r.Errors, err.Error())
}

func (s *service) Tail(ctx context.Context, request *TailRequest) (*TailResponse, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	rule, err := s.loadRule(ctx, request.RuleURL)
	if err != nil {
		return nil, err
	}
	ruleMap := map[string]interface{}{}
	toolbox.DefaultConverter.AssignConverted(&ruleMap, rule)
	ruleMap = toolbox.DeleteEmptyKeys(ruleMap)
	ruleYAML, err := yaml.Marshal(ruleMap)
	if err == nil {
		shared.LogF("using rule:\n%s\n", ruleYAML)
	}
	object, err := s.fs.Object(ctx, request.SourceURL)
	if err != nil {
		return nil, errors.Wrapf(err, "source location not found: %v", request.SourceURL)
	}

	response := &TailResponse{}
	ctx, cancel := context.WithCancel(ctx)
	go s.tailInBackground(ctx, cancel)
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	go s.ingestDatafiles(ctx, waitGroup, object, rule, response)
	go s.handleResponse(ctx, response)
	waitGroup.Wait()

	for atomic.LoadInt32(&s.stopped) == 0 && atomic.LoadInt32(&response.pending) > 0 {
		time.Sleep(2 * time.Second)
		shared.LogProgress()
	}
	s.Stop()
	return response, err
}

func (s *service) handleResponse(ctx context.Context, response *TailResponse) {
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
					atomic.AddInt32(&response.pending, -int32(len(window.URIs)))
				}
			}
			if resp.Window == nil && resp.Matched {
				atomic.AddInt32(&response.pending, -1)
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
