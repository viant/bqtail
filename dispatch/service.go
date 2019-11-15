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
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Service represents event service
type Service interface {
	Dispatch(ctx context.Context) *contract.Response
	Config() *Config
}

type service struct {
	task.Registry
	config *Config
	fs     afs.Service
	bq     bq.Service
}

//Config returns service config
func (s *service) Config() *Config {
	return s.config
}

//BQService returns bq service
func (s *service) BQService() bq.Service {
	return s.bq
}

func (s *service) Init(ctx context.Context) error {
	err := s.config.Init(ctx, s.fs)
	if err != nil {
		return err
	}
	slackService := slack.New(s.config.Region, s.config.ProjectID, s.fs, secret.New(), s.config.SlackCredentials)
	slack.InitRegistry(s.Registry, slackService)
	bqService, err := bigquery.NewService(ctx)
	if err != nil {
		return err
	}
	s.bq = bq.New(bqService, s.Registry, s.config.ProjectID, s.fs, s.config.Config)
	bq.InitRegistry(s.Registry, s.bq)
	storage.InitRegistry(s.Registry, storage.New(s.fs))
	return err
}


//Dispatch dispatched BigQuery event
func (s *service) Dispatch(ctx context.Context) *contract.Response {
	response := contract.NewResponse()
	defer response.SetTimeTaken(response.Started)
	timeToLive := s.config.TimeToLive()
	startTime := time.Now()
	for ;; {
		err := s.dispatch(ctx, response)
		if err != nil {
			response.SetIfError(err)
		}
		if time.Now().Sub(startTime) > timeToLive {
			break
		}
		time.Sleep(time.Second)
	}
	return response
}



func (s *service) getJobs(ctx context.Context, response *contract.Response) ([]*Job, error) {
	var result = make([]*Job, 0)
	jobMatcher, _ := matcher.NewBasic("", fmt.Sprintf("%v.json", base.DispatchJob), "", nil)
	candidate, err := s.fs.List(ctx, s.Config().DeferTaskURL, option.NewRecursive(true), jobMatcher)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list jobs %v", s.Config().DeferTaskURL)
	}
	for i := range candidate {
		if candidate[i].IsDir() {
			continue
		}
		jobID := JobID(s.Config().DeferTaskURL, candidate[i].URL())
		bqJob, err := s.bq.GetJob(ctx, s.config.ProjectID, jobID)
		if err != nil {
			response.AddError(errors.Wrapf(err, "failed to get bqJob: %v\n",candidate[i].URL()))
			continue

		}
		if bqJob.Status != nil && bqJob.Status.State == base.DoneStatus {
			job, err := s.loadJob(ctx, candidate[i].URL(), bqJob)
			if err != nil {
				response.AddError(err)
				continue
			}
			result = append(result, job)
		}
	}
	//TODO add jobs that cam be matched by source or dest
	return result, nil
}



func (s *service) loadJob(ctx context.Context, URL string,  job *bigquery.Job) (*Job, error) {
	baseJob := base.Job(*job)
	reader, err := s.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download job: %v\n", URL)

	}
	defer func() {
		_ = reader.Close()
	}()
	actions := &task.Actions{}
	if err = json.NewDecoder(reader).Decode(actions); err != nil {
		return nil,  errors.Wrapf(err, "failed to decode job: %v\n", URL)
	}
	return  NewJob(URL, &baseJob, actions), nil
}


func (s *service) dispatch(ctx context.Context, response *contract.Response) (err error) {
	jobs , err := s.getJobs(ctx, response)
	if err != nil {
		return
	}
	response.Jobs = jobs
	for _, job := range jobs {
		err = s.notify(ctx, job)
		if err != nil {
			response.AddError(err)
		}
	}
	return nil
}



func (s *service) notify(ctx context.Context, job *Job) error {
	if exists, _ := s.fs.Exists(ctx, job.URL) ; !exists {
		return nil
	}
	toRun := job.Actions.ToRun(job.Error(), job.Job, s.config.DeferTaskURL)
	if len(toRun) > 0 {
		actionURL := s.config.BuildActionURL(job.Job.JobReference.JobId)
		JSON, err := json.Marshal(toRun)
		if err != nil {
			return err
		}
		if err = s.fs.Upload(ctx, actionURL, 0644, bytes.NewReader(JSON));err != nil {
			return err
		}
	}
	return s.moveJob(ctx, job)
}


func (s *service) moveJob(ctx context.Context, job *Job) error {
	baseURL := s.config.OutputURL(job.Error() != nil)
	sourceURL := job.URL
	_, sourcePath := url.Base(sourceURL, "")
	if len(s.config.DeferTaskURL) < len(sourceURL) {
		sourcePath = string(sourceURL[len(s.config.DeferTaskURL): ])
	}
	destURL := url.Join(baseURL, sourcePath)
	return s.fs.Move(ctx, sourceURL, destURL)
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
