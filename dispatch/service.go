package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/sortable"
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

	running := int32(1)
	timeoutDuration = timeoutDuration - thinkTime
	cycleStartTime := time.Now()

	var perf *contract.Performance
	for atomic.LoadInt32(&running) == 1 {
		response.Reset()
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(2)
		objects, err := s.fs.List(ctx, s.config.AsyncTaskURL)
		if perf != nil {
			fmt.Printf("processed: %v, dispatched: {load: %v, copy:%v, query: %v}, pending: {load:%v, copy: %v,  query: %v}, running: {load : %v, copy: %v, query: %v}, batched: %v\n", len(objects), perf.Dispatched.LoadJobs, perf.Dispatched.CopyJobs, perf.Dispatched.QueryJobs, perf.Pending.LoadJobs, perf.Pending.CopyJobs, perf.Pending.QueryJobs, perf.Running.LoadJobs, perf.Running.CopyJobs, perf.Running.QueryJobs, len(response.Batched))
		}
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
			perf, err = s.dispatchBqEvents(ctx, response, objects)
			if IsContextError(err) || IsNotFound(err) || err == nil {
				response.Performance.Merge(perf)
				return
			}
			response.SetIfError(err)
		}(objects)
		response.Cycles++
		select {
		case <-time.After(timeoutDuration):
			continue
		case <-ctx.Done():
			atomic.StoreInt32(&running, 0)
			return nil

		case <-func() chan bool {
			boolChannel := make(chan bool, 1)
			go func() {
				waitGroup.Wait()
				boolChannel <- true
			}()
			return boolChannel
		}():

			timeoutDuration -= time.Now().Sub(cycleStartTime)
			cycleStartTime = time.Now()
			if timeoutDuration < 0 {
				return nil
			}
		}

		select {
		case <-time.After(thinkTime):
		case <-ctx.Done():
			atomic.StoreInt32(&running, 0)
			return nil
		}
	}
	return nil
}

func (s *service) notifyDoneProcesses(ctx context.Context, objects []astorage.Object, response *contract.Response, jobsByID *jobs, perf *contract.Performance) (err error) {
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
		listJob := jobsByID.get(jobID)
		if listJob != nil {
			state = listJob.State

		} else {
			response.GetCount++
			job, err := s.bq.GetJob(ctx, s.config.ProjectID, jobID)
			if err != nil || job == nil {
				perf.MissingStatus++
				continue
			}
			if job.Status != nil {
				state = job.Status.State
			}
		}
		perf.AddEvent(state, jobID)
		switch strings.ToUpper(state) {
		case base.DoneState:
			break
		default:
			continue
		}

		stageInfo := perf.AddDispatch(jobID)
		if !s.canNotify(stageInfo, perf) {
			perf.AddThrottled(jobID)
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

func (s *service) canNotify(info *stage.Info, perf *contract.Performance) bool {
	if info.Action == "query" {
		return s.config.MaxConcurrentSQL == 0 || s.config.MaxConcurrentSQL > perf.ActiveQueryCount()+1
	}
	return s.config.MaxConcurrentJobs == 0 || s.config.MaxConcurrentJobs > perf.ActiveJobCount()+1
}

func (s *service) dispatchBqEvents(ctx context.Context, response *contract.Response, objects []astorage.Object) (*contract.Performance, error) {
	var jobsByID = newJobs()

	startTime := time.Now()
	perf := contract.NewPerformance()
	listLoopback := time.Minute * time.Duration(s.config.MaxJobLoopbackInMin)
	modifiedAfter := startTime.Add(-listLoopback)

	if err := s.listBQJobs(ctx, modifiedAfter, objects, perf, jobsByID); err != nil {
		return perf, nil
	}

	sorted := sortable.NewObjects(objects, sortable.ByModTime)
	modified := sorted.Elements[len(sorted.Elements)-1].ModTime()
	maxLoopback := time.Now().Sub(modified)
	if maxLoopback > listLoopback {
		go func() {
			modifiedAfter := startTime.Add(-maxLoopback)
			_ = s.listBQJobs(ctx, modifiedAfter, objects, perf, jobsByID)
		}()
	}
	response.ListTime = fmt.Sprintf("%s", time.Now().Sub(startTime))
	return perf, s.notifyDoneProcesses(ctx, sorted.Elements, response, jobsByID, perf)
}

func (s *service) listBQJobs(ctx context.Context, modifiedAfter time.Time, objects []astorage.Object, perf *contract.Performance, jobsByID *jobs) error {
	jobs, err := s.bq.ListJob(ctx, s.config.ProjectID, modifiedAfter, "done", "pending", "running")
	if err != nil || len(objects) == 0 {
		return err
	}
	for i := range jobs {
		jobsByID.put(jobs[i])
	}
	return err
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
	response.BatchCount = len(objects)
	for _, obj := range objects {
		if obj.IsDir() || path.Ext(obj.Name()) != base.WindowExt {
			continue
		}
		if response.HasBatch(obj.URL()) {
			continue
		}
		dueTime, e := URLToWindowEndTime(obj.URL())
		if e != nil {
			err = e
			continue
		}

		if time.Now().After(dueTime.Add(base.StorageListVisibilityDelay * time.Millisecond)) {
			response.AddBatch(obj.URL(), *dueTime)
			baseURL := fmt.Sprintf("gs://%v%v", s.config.TriggerBucket, s.config.BatchPrefix)
			destURL := url.Join(baseURL, obj.Name())
			if e := s.fs.Move(ctx, obj.URL(), destURL, option.NewObjectKind(true)); e != nil {
				err = e
			}
		}
	}
	return err
}

//JobID returns job ID for supplied URL
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
