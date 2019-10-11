package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"path"
	"strings"
)

//Service represents event service
type Service interface {
	Dispatch(ctx context.Context, request *contract.Request) *contract.Response
}

type service struct {
	task.Registry
	config *Config
	bq     bq.Service
	fs     afs.Service
}


func (s *service) Init(ctx context.Context) error {
	err := s.config.Init(ctx, s.fs)
	if err != nil {
		return err
	}
	slackService := slack.New(s.config.Region, s.config.ProjectID, s.fs, secret.New())
	slack.InitRegistry(s.Registry, slackService)
	bqService, err := bigquery.NewService(ctx)
	if err != nil {
		return err
	}
	s.bq = bq.New(bqService, s.Registry, s.config.ProjectID, s.fs)
	bq.InitRegistry(s.Registry, s.bq)
	storage.InitRegistry(s.Registry, storage.New(s.fs))

	return err
}


func (s *service) Dispatch(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	defer response.SetTimeTaken(response.Started)
	err := s.dispatch(ctx, request, response)
	if err != nil {
		response.SetIfError(err)
	}
	return response
}


func (s *service) initRequest(ctx context.Context, request *contract.Request) error {
	job, err := s.bq.GetJob(ctx, request.ProjectID, request.JobID)
	if err != nil {
		return err
	}
	contractJob := base.Job(*job)
	request.Job = &contractJob
	return nil
}


//move moves schedule file to output folder
func (s *service) move(ctx context.Context, baseURL string, job *Job) error {
	matchedURL := job.Response.MatchedURL
	if matchedURL == "" {
		return nil
	}
	parent, sourceName := url.Split(matchedURL, file.Scheme)
	parentElements := strings.Split(parent, "/")
	if len(parentElements) > 3 {
		sourceName = path.Join(strings.Join(parentElements[len(parentElements)-3:], "/"), sourceName)
	}
	name := path.Join(job.Completed().Format(dateLayout), sourceName)
	URL := url.Join(baseURL, name)
	return s.fs.Move(ctx, matchedURL, URL)
}


func (s *service) onDone(ctx context.Context, job *Job) error {
	baseURL := s.config.OutputURL(job.Response.Status == base.StatusError)
	if err := s.move(ctx, baseURL, job); err != nil {
		return err
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	jobFilename := path.Join(base.DecodePathSeparator(job.JobReference.JobId))
	name := path.Join(job.Completed().Format(dateLayout), jobFilename+base.JobElement+base.JobExt)
	URL := url.Join(baseURL, name)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}


func (s *service) getActions(ctx context.Context, request *contract.Request, response *contract.Response) (*task.Actions, error) {
	jobID := base.DecodePathSeparator(request.JobID)
	if strings.HasSuffix(jobID, base.DispatchJob) {
		URL := url.Join(s.config.DeferTaskURL, jobID+base.JobExt)
		response.MatchedURL = URL
		response.Matched = true
		reader, err := s.fs.DownloadWithURL(ctx, URL)
		if err != nil {
			return nil, err
		}
		defer func() { _ = reader.Close() }()
		actions := &task.Actions{}
		return actions, json.NewDecoder(reader).Decode(actions)
	}

	return nil, nil
}


func (s *service) dispatch(ctx context.Context, request *contract.Request, response *contract.Response) (err error) {
	err = s.initRequest(ctx, request)
	if err != nil {
		return err
	}
	job := NewJob(request.Job, response)
	defer func() {
		if ! response.Matched {
			response.Status = base.StatusNoMatch
		}
		if response.Matched || err != nil {
			response.SetIfError(err)
			if e := s.onDone(ctx, job); e != nil && err == nil {
				e = err
			}
		}
	}()
	response.JobRef = request.Job.JobReference
	if err := request.Job.Error(); err != nil {
		response.JobError = err.Error()
	}
	if err = s.config.ReloadIfNeeded(ctx, s.fs); err != nil {
		return err
	}
	rule := s.config.Match(request.Job)
	if rule != nil {
		response.Rule = rule
		expandable := &base.Expandable{}
		if rule.When.Dest != "" {
			expandable.Source = request.Job.Dest()
		} else if rule.When.Source != "" {
			expandable.Source = request.Job.Source()
		}
		response.Matched = true
		job.Actions = rule.Actions.Expand(expandable)
		return s.run(ctx, job)
	}
	job.Actions, err = s.getActions(ctx, request, response)
	if err != nil || job.Actions == nil || job.Actions.IsEmpty() {
		return err
	}
	err = s.run(ctx, job)
	return err
}

func (s *service) run(ctx context.Context, job *Job) error {
	toRun := job.Actions.ToRun(job.Error(), job.Job)
	var err error
	for i := range toRun {
		if err = task.Run(ctx, s.Registry, toRun[i]); err != nil {
			return err
		}
	}
	return err
}

//New creates a dispatch service
func New(ctx context.Context, config *Config) (Service, error) {
	srv := &service{
		config:   config,
		fs:       afs.New(),
		Registry: task.NewRegistry(),
	}
	return srv, srv.Init(ctx)
}

