package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//Service represents event service
type Service interface {
	Dispatch(ctx context.Context) *contract.Response
	Config() *Config
}


type service struct {
	task.Registry
	lastCheck *time.Time
	config    *Config
	fs        afs.Service
	bq        bq.Service
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
	for ; ; {
		response.Reset()
		err := s.dispatch(ctx, response)
		if err != nil {
			response.SetIfError(err)
		}
		response.Cycles++
		if time.Now().Sub(startTime) > timeToLive {
			break
		}
		time.Sleep(time.Second)
	}
	return response
}

func (s *service) processJobs(ctx context.Context, response *contract.Response) error {
	startTime := time.Now()
	modifiedAfter := startTime.Add(- (time.Minute * time.Duration(s.config.MaxJobLoopbackInMin)))
	jobs, err := s.bq.ListJob(ctx, s.config.ProjectID, modifiedAfter, "done", "pending", "running")
	if err != nil {
		return err
	}
	response.ListTime = fmt.Sprintf("%s", time.Now().Sub(startTime))
	var jobsByID = make(map[string]*bigquery.JobListJobs)
	for i := range jobs {
		jobsByID[jobs[i].JobReference.JobId] = jobs[i]
	}
	response.ListCount = len(jobsByID)
	return s.processURL(ctx, s.config.DeferTaskURL, response, jobsByID)
}


func (s *service) processURL(ctx context.Context, parentURL string, response *contract.Response, jobsByID map[string]*bigquery.JobListJobs) error {
	objects, err := s.fs.List(ctx, parentURL)
	if err != nil {
		return err
	}

	waitGroup := &sync.WaitGroup{}
	for i := range objects {
		URL := url.Join(parentURL, objects[i].Name())
		if url.Equals(URL, parentURL) {
			continue
		}
		if objects[i].IsDir() {
			if err = s.processURL(ctx, URL, response, jobsByID); err != nil {
				return err
			}
		}
		if response.Jobs.Has(URL) {
			continue
		}
		jobID := JobID(s.Config().DeferTaskURL, URL)
		var state string
		listJob, ok := jobsByID[jobID]
		if ok {
			state = listJob.State
		} else {
			response.GetCount++
			job, err := s.bq.GetJob(ctx, s.config.ProjectID, jobID)
			if err != nil || job == nil {
				atomic.AddInt32(&response.MissingCount, 1)
				continue
			}
			if job.Status != nil {
				state = job.Status.State
			}
		}

		switch strings.ToUpper(state){
		case base.RunningState:
			atomic.AddInt32(&response.RunningCount, 1)
			continue
		case base.PendigState:
			atomic.AddInt32(&response.PendingCount, 1)
			continue
		}
		job := contract.NewJob(jobID, URL, state)
		waitGroup.Add(1)
		go func(job *contract.Job) {
			err = s.notify(ctx, job)
			defer waitGroup.Done()
			if err = s.notify(ctx, job); err != nil {
				response.Jobs.Add(job)
			}
		}(job)
	}
	waitGroup.Wait()
	return err
}

func (s *service) dispatch(ctx context.Context, response *contract.Response) (err error) {
	return s.processJobs(ctx, response)
}

//notify notify bqtail
func (s *service) notify(ctx context.Context, job *contract.Job) error {
	if exists, _ := s.fs.Exists(ctx, job.URL); !exists {
		return nil
	}
	taskURL := s.config.BuildTaskURL(job.ID)
	return  s.fs.Move(ctx, job.URL, taskURL)
}


//JobID returns job ID for supplied URL
func JobID(baseURL string, URL string) string {
	if len(baseURL) > len(URL) {
		return ""
	}
	encoded := strings.Trim(string(URL[len(baseURL):]), "/")
	encoded = strings.Replace(encoded, ".json", "", 1)
	jobID := base.EncodePathSeparator(encoded)
	return jobID
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
