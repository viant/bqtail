package mon

import (
	"bqtail/base"
	"bqtail/tail/config"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	_ "github.com/viant/afsc/gs"
	"io/ioutil"
	"strings"
	"time"
)

//Service represents monitoring service
type Service interface {
	//Check checks un process file and mirror errors
	Check(context.Context, *Request) *Response
}

type service struct {
	fs afs.Service
	*Config
}

//Check checks triggerBucket and error
func (s *service) Check(ctx context.Context, request *Request) *Response {
	response := NewResponse()
	err := s.check(ctx, request, response)
	if err != nil {
		response.Error = err.Error()
		response.Status = base.StatusError
	} else if response.UnprocessedCount > 0 {
		response.Status = base.StatusUnProcess
	} else if len(response.Errors) > 0 {
		response.Status = base.StatusError
		response.Error = response.Errors[0].Message
	}
	return response
}

func (s *service) check(ctx context.Context, request *Request, response *Response) (err error) {
	if err = request.Init(); err != nil {
		return err
	}
	if request.ErrorURL != "" {
		if err = s.checkErrors(ctx, request, response); err != nil {
			return err
		}
	}

	if err = s.checkBatches(ctx, request, response); err != nil {
		return err
	}
	if err = s.checkDeferTasks(ctx, request, response); err != nil {
		return err
	}
	if request.ProcessedURL != "" {
		if err = s.checkProcessed(ctx, request, response); err != nil {
			return err
		}
	}
	return s.checkUnprocessed(ctx, request, response)
}

func (s *service) list(ctx context.Context, URL string, modifiedBefore, modifiedAfter *time.Time) ([]storage.Object, error) {
	timeMatcher := matcher.NewModification(modifiedBefore, modifiedAfter)
	recursive := option.NewRecursive(true)
	exists, _ := s.fs.Exists(ctx, URL)
	if !exists {
		return []storage.Object{}, nil
	}
	return s.fs.List(ctx, URL, timeMatcher, recursive)
}

func (s *service) checkDeferTasks(ctx context.Context, request *Request, response *Response) error {
	cfg, err := NewConfig(ctx, request.ConfigURL, s.fs)
	if err != nil {
		return errors.Wrapf(err, "failed to load cfg: %v", request.ConfigURL)
	}
	response.DeferTasks = make([]*File, 0)
	objects, err := s.list(ctx, cfg.AsyncTaskURL, request.unprocessedModifiedBefore, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to check errors: %v", cfg.AsyncTaskURL)
	}
	now := time.Now()
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		response.AddDeferTask(now, object)
	}
	return nil
}

func (s *service) checkBatches(ctx context.Context, request *Request, response *Response) error {
	cfg, err := NewConfig(ctx, request.ConfigURL, s.fs)
	if err != nil {
		return errors.Wrapf(err, "failed to load cfg: %v", request.ConfigURL)
	}
	response.DeferTasks = make([]*File, 0)
	objects, err := s.list(ctx, cfg.BatchURL, request.unprocessedModifiedBefore, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to check batched: %v", cfg.BatchURL)
	}
	now := time.Now()
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		response.AddDeferTask(now, object)
	}
	return nil
}

func (s *service) checkErrors(ctx context.Context, request *Request, response *Response) error {
	objects, err := s.list(ctx, request.ErrorURL, nil, request.errorModifiedAfter)
	if err != nil {
		return errors.Wrapf(err, "failed to check errors: %v", request.ErrorURL)
	}
	for _, object := range objects {
		if object.IsDir() {
			continue
		}

		hasErrorMessage := strings.HasSuffix(object.URL(), "-error")
		message := []byte{}
		if hasErrorMessage {
			reader, err := s.fs.Download(ctx, object)
			if err != nil {
				return err
			}
			message, err := ioutil.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				return err
			}
			if len(message) > 150 {
				message = message[:150]
			}
		}
		response.AddError(object, string(message))
	}
	response.ErrorCount = len(response.Errors)
	return nil
}

func (s *service) loadRuleset(ctx context.Context, URL string) (*config.Ruleset, error) {
	reader, err := s.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	result := &config.Ruleset{}
	err = json.NewDecoder(reader).Decode(&result)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode ruleset: %v", URL)
	}
	return result, err
}

func (s *service) checkProcessed(ctx context.Context, request *Request, response *Response) error {
	rulesets, err := s.loadRuleset(ctx, request.ConfigURL)
	if err != nil {
		return errors.Wrapf(err, "failed to load rulesets: configf from URL :%v", request.ConfigURL)
	}
	if err := rulesets.Init(ctx, s.fs, s.Config.ProjectID); err != nil {
		return err
	}
	objects, err := s.list(ctx, request.ProcessedURL, nil, request.processedModifiedAfter)
	if err != nil {
		return errors.Wrapf(err, "failed to check processed: %v", request.ProcessedURL)
	}
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		routes := rulesets.Match(object.URL())
		var route *config.Rule
		if len(routes) == 1 {
			route = routes[0]
		}
		response.AddProcessed(route, object)
	}
	return nil
}

func (s *service) checkUnprocessed(ctx context.Context, request *Request, response *Response) error {
	routes, err := s.loadRuleset(ctx, request.ConfigURL)
	if err != nil {
		return errors.Wrapf(err, "failed to load routes: %v", request.ConfigURL)
	}
	err = routes.Init(ctx, s.fs, s.Config.ProjectID)
	if err != nil {
		return err
	}

	objects, err := s.list(ctx, request.TriggerURL, request.unprocessedModifiedBefore, nil)

	if err != nil {
		return errors.Wrapf(err, "failed to check unprocessed: %v", request.TriggerURL)
	}
	now := time.Now()
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		var rule *config.Rule
		rules := routes.Match(object.URL())
		if len(rules) == 1 {
			rule = rules[0]
		}
		response.AddUnprocessed(now, rule, object)
	}
	return nil
}

//New creates monitoring service
func New(ctx context.Context, config *Config) (Service, error) {
	err := config.Init(ctx)
	if err != nil {
		return nil, err
	}
	return &service{
		fs:     afs.New(),
		Config: config,
	}, err
}
