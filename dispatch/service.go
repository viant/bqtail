package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"

	"bqtail/stage"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	astorage "github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var thinkTime = 1500 * time.Millisecond

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
	err := s.dispatch(ctx, response)
	if err != nil {
		response.SetIfError(err)
	}
	return response
}

//Dispatch dispatched BigQuery event
func (s *service) dispatch(ctx context.Context, response *contract.Response) error {
	timeInSec := toolbox.AsInt(os.Getenv("FUNCTION_TIMEOUT_SEC"))
	remainingDuration := time.Duration(timeInSec)*time.Second - thinkTime
	timeoutDuration := s.config.TimeToLive()
	if timeoutDuration > remainingDuration {
		timeoutDuration = remainingDuration
	}
	ctx, cancelFunc := context.WithTimeout(ctx, timeoutDuration)
	defer cancelFunc()
	sleepTime := 2 * thinkTime
	running := int32(1)
	for atomic.LoadInt32(&running) == 1 {
		response.Reset()
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(2)
		objects, err := s.fs.List(ctx, s.config.AsyncTaskURL)
		if err != nil {
			if IsContextError(err) || IsNotFound(err) {
				err = nil
			}
			return err
		}
		go func(objects []astorage.Object) {
			defer waitGroup.Done()
			e := s.dispatchBatchEvents(ctx, response, objects)
			if IsContextError(e) || IsNotFound(e) {
				return
			}
			if e != nil {
				response.SetIfError(e)
			}
		}(objects)

		go func(objects []astorage.Object) {
			defer waitGroup.Done()
			err := s.dispatchBqEvents(ctx, response, objects)
			if IsContextError(err) || IsNotFound(err) {
				return
			}
			if err != nil {
				response.SetIfError(err)
			}

		}(objects)
		response.Cycles++

		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			atomic.StoreInt32(&running, 0)
			return nil
		case <-func() chan bool {
			boolChannel := make(chan bool)
			go func() {
				waitGroup.Wait()
				boolChannel <- true
			}()
			return boolChannel
		}():
		}
	}
	return nil
}

func (s *service) notifyDoneJobs(ctx context.Context, objects []astorage.Object, response *contract.Response, jobsByID map[string]*bigquery.JobListJobs) (err error) {
	waitGroup := &sync.WaitGroup{}
	for i, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) == base.WindowExt {
			continue
		}
		age := time.Now().Sub(objects[i].ModTime())
		//if just create skip to next
		if age < thinkTime {
			continue
		}
		if response.Jobs.Has(object.URL()) {
			continue
		}
		jobID := JobID(s.Config().AsyncTaskURL, object.URL())
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
		switch strings.ToUpper(state) {
		case base.RunningState:
			atomic.AddInt32(&response.RunningCount, 1)
			continue
		case base.PendigState:
			atomic.AddInt32(&response.PendingCount, 1)
			continue
		}
		job := contract.NewJob(jobID, object.URL(), state)
		waitGroup.Add(1)
		go func(job *contract.Job) {
			defer waitGroup.Done()
			err = s.notify(ctx, job)
			if err == nil {
				response.Jobs.Add(job)
			} else {
				response.AddError(err)
			}
		}(job)
	}
	waitGroup.Wait()
	return err
}

func (s *service) dispatchBqEvents(ctx context.Context, response *contract.Response, objects []astorage.Object) (err error) {
	startTime := time.Now()
	modifiedAfter := startTime.Add(-(time.Minute * time.Duration(s.config.MaxJobLoopbackInMin)))
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
	return s.notifyDoneJobs(ctx, objects, response, jobsByID)

}

//notify notify bqtail
func (s *service) notify(ctx context.Context, job *contract.Job) error {
	info := stage.Parse(job.ID)
	taskURL := s.config.BuildTaskURL(info)
	return s.fs.Move(ctx, job.URL, taskURL, option.NewObjectKind(true))
}

func (s *service) dispatchBatchEvents(ctx context.Context, response *contract.Response, objects []astorage.Object) (err error) {
	if len(objects) == 0 {
		return nil
	}
	response.BatchCount += len(objects)
	for _, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) != base.WindowExt {
			continue
		}
		if response.HasBatch(object.URL()) {
			continue
		}
		dueTime, e := URLToWindowEndTime(object.URL())
		if e != nil {
			err = e
			continue
		}

		if time.Now().After(dueTime.Add(base.StorageListVisibilityDelay * time.Millisecond)) {
			response.AddBatch(object.URL(), *dueTime)
			baseURL := fmt.Sprintf("gs://%v%v", s.config.TriggerBucket, s.config.BatchPrefix)
			destURL := url.Join(baseURL, object.Name())
			if e := s.fs.Move(ctx, object.URL(), destURL, option.NewObjectKind(true)); e != nil {
				err = e
			}
		}
	}
	return err
}

//GetJobID returns job ID for supplied URL
func JobID(baseURL string, URL string) string {
	if len(baseURL) > len(URL) {
		return ""
	}
	encoded := strings.Trim(string(URL[len(baseURL):]), "/")
	encoded = strings.Replace(encoded, ".json", "", 1)
	jobID := stage.Decode(encoded)
	return jobID
}

//New creates a dispatchBqEvents service
func New(ctx context.Context, config *Config) (Service, error) {
	srv := &service{
		config:   config,
		fs:       afs.New(),
		Registry: task.NewRegistry(),
	}
	return srv, srv.Init(ctx)
}
