package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/dispatch/project"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/shared"
	"bqtail/sortable"
	"bqtail/stage"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
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
	if timeoutDuration > remainingDuration && remainingDuration > 0 {
		timeoutDuration = remainingDuration
	}
	ctx, cancelFunc := context.WithTimeout(ctx, timeoutDuration)
	defer cancelFunc()

	running := int32(1)
	timeoutDuration = timeoutDuration - thinkTime

	for atomic.LoadInt32(&running) == 1 {
		cycleStartTime := time.Now()
		waitGroup := &sync.WaitGroup{}
		projectEvents, err := s.listProjectEvents(ctx)
		for i := range projectEvents {
			fmt.Printf("Processing: %v:%v %v\n", projectEvents[i].Region, projectEvents[i].ProjectID, len(projectEvents[i].Items))
		}
		if isProcessingError(err) {
			return err
		}
		for i := range projectEvents {
			waitGroup.Add(1)
			go s.dispatchEvents(ctx, waitGroup, response, projectEvents[i])
		}
		response.Cycles++
		if err = s.logPerformance(ctx, response); err != nil {
			fmt.Printf("%v\n", err)
		}
		if !s.wait(ctx, cycleStartTime, waitGroup, &running, &timeoutDuration) {
			break
		}
		for i := range projectEvents {
			fmt.Printf("%v\n", projectEvents[i].Performance)
		}
	}
	return nil
}

func (s *service) listProjectEvents(ctx context.Context) ([]*project.Events, error) {
	registry := project.NewRegistry()
	events, err := s.fs.List(ctx, s.config.AsyncTaskURL)
	if err != nil {
		return nil, err
	}

	addEvents(s.config.ProjectID, events, registry)
	for _, obj := range events {
		if obj.IsDir() && strings.HasPrefix(obj.Name(), shared.TempProjectPrefix) {
			projectRegion := string(obj.Name()[len(shared.TempProjectPrefix):])
			projectEvents, err := s.fs.List(ctx, obj.URL())
			if err != nil {
				return nil, err
			}
			addEvents(projectRegion, projectEvents, registry)
		}
	}
	return registry.Events(), nil
}

func addEvents(projectID string, events []astorage.Object, registry *project.Registry) {
	for i, event := range events {
		if event.IsDir() {
			continue
		}
		registry.Add(projectID, events[i])
	}
}

func (s *service) wait(ctx context.Context, startTime time.Time, waitGroup *sync.WaitGroup, running *int32, remaining *time.Duration) bool {
	select {
	case <-time.After(*remaining):
		return false
	case <-ctx.Done():
		atomic.StoreInt32(running, 0)
		return false
	case <-func() chan bool {
		boolChannel := make(chan bool, 1)
		go func() {
			waitGroup.Wait()
			boolChannel <- true
		}()
		return boolChannel
	}():
		*remaining -= time.Now().Sub(startTime)
		if *remaining <= 0 {
			return false
		}
	}

	select {
	case <-time.After(2 * thinkTime):
	case <-ctx.Done():
		atomic.StoreInt32(running, 0)
		return false
	}
	return true
}

func (s *service) dispatchEvents(ctx context.Context, waitGroup *sync.WaitGroup, response *contract.Response, projectEvents *project.Events) {
	defer waitGroup.Done()
	err := s.dispatchBqEvents(ctx, response, projectEvents)
	if err == nil || IsNotFound(err) {
		err = s.dispatchBatchEvents(ctx, response, projectEvents)
	}
	if IsContextError(err) || IsNotFound(err) || err == nil {
		response.Merge(projectEvents.Performance)
		return
	}
	response.SetIfError(err)
}

func (s *service) logPerformance(ctx context.Context, response *contract.Response) error {
	URL := url.Join(s.config.JournalURL, shared.PerformanceFile)
	JSON, err := json.Marshal(response.Performance)
	if err != nil {
		return err
	}
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(JSON))
	return err
}

func (s *service) filterCandidate(response *contract.Response, objects []astorage.Object, action string) map[string][]astorage.Object {
	var result = make(map[string][]astorage.Object, 0)
	for i, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) == shared.WindowExt {
			continue
		}
		if response.Jobs.Has(object.URL()) {
			continue
		}

		age := time.Now().Sub(objects[i].ModTime())
		//if just create skip to next
		if age < thinkTime {
			continue
		}
		jobID := JobID(s.Config().AsyncTaskURL, object.URL())
		stageInfo := stage.Parse(jobID)
		if stageInfo.Action != action {
			continue
		}
		if _, ok := result[stageInfo.DestTable]; !ok {
			result[stageInfo.DestTable] = []astorage.Object{}
		}
		result[stageInfo.DestTable] = append(result[stageInfo.DestTable], objects[i])
	}
	return result
}

func (s *service) notifyDoneProcesses(ctx context.Context, events *project.Events, response *contract.Response, jobsByID *jobs) (err error) {
	waitGroup := &sync.WaitGroup{}
	for i, object := range events.Items {
		if object.IsDir() || path.Ext(object.Name()) == shared.WindowExt {
			continue
		}

		age := time.Now().Sub(events.Items[i].ModTime())
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
			job, err := s.bq.GetJob(ctx, events.Region, events.ProjectID, jobID)
			if err != nil || job == nil {
				if time.Now().Sub(object.ModTime()) > 30*time.Minute {
					fmt.Printf("removing: %v %v %v due to not found\n", events.Region, events.ProjectID, jobID)
					//If not job has been found for 5 min - delete file otherwise checking too many non existing jobs can clog dispatcher
					s.fs.Delete(ctx, object.URL())
				}
				events.NoFound++
				continue
			}
			if job.Status != nil {
				state = job.Status.State
			}
		}

		switch strings.ToUpper(state) {
		case shared.DoneState:
			break
		default:
			events.AddEvent(state, jobID)
			continue
		}
		stageInfo := events.AddDispatch(jobID)
		if !s.canNotify(stageInfo.Action, events.Performance) {
			events.AddThrottled(jobID)
			continue
		}
		job := contract.NewJob(jobID, object.URL(), state)

		waitGroup.Add(1)
		go func(job *contract.Job) {
			defer waitGroup.Done()
			err = s.notify(ctx, job, events)
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

func (s *service) canNotify(action string, perf *contract.Performance) bool {
	if action == shared.ActionQuery {
		return s.config.MaxConcurrentSQL == 0 || s.config.MaxConcurrentSQL > perf.ActiveQueryCount()+1
	}
	if action == shared.ActionLoad {
		return s.config.MaxConcurrentLoad == 0 || s.config.MaxConcurrentLoad > perf.ActiveLoadCount()+1
	}
	return true
}

func (s *service) dispatchBqEvents(ctx context.Context, response *contract.Response, events *project.Events) error {
	var jobsByID = newJobs()
	stepDuration := 10 * time.Minute
	maxCreated := time.Now()
	minCreated := maxCreated.Add(-stepDuration)
	perf := events.Performance
	if len(events.Items) == 0 {
		return nil
	}
	sorted := sortable.NewObjects(events.Items, sortable.ByModTime)
	events.Items = sorted.Elements
	minListTime := sorted.Elements[len(sorted.Elements)-1].ModTime()
	startTime := time.Now()
	waitGroup := &sync.WaitGroup{}
	for {
		waitGroup.Add(1)
		go func(minCreated, maxCreated time.Time) {
			defer waitGroup.Done()
			s.listBQJobs(ctx, perf.ProjectID, minCreated, maxCreated, jobsByID)
		}(minCreated, maxCreated)
		maxCreated = maxCreated.Add(-stepDuration)
		minCreated = minCreated.Add(-stepDuration)
		candidate := minCreated.Add(-stepDuration)
		if candidate.Before(minListTime) {
			break
		}
	}
	go func() {
		waitGroup.Wait()
		response.ListTime = fmt.Sprintf("%s", time.Now().Sub(startTime))
	}()
	err := s.notifyDoneProcesses(ctx, events, response, jobsByID)
	return err
}

func (s *service) listBQJobs(ctx context.Context, projectID string, minCreated, maxCreated time.Time, jobsByID *jobs) {
	jobs, err := s.bq.ListJob(ctx, projectID, minCreated, maxCreated, "done", "pending", "running")
	if err != nil {
		return
	}
	for i := range jobs {
		jobsByID.put(jobs[i])
	}
}

//notify notify bqtail
func (s *service) notify(ctx context.Context, job *contract.Job, events *project.Events) error {
	info := stage.Parse(job.ID)
	info.Region = events.Region
	info.ProjectID = events.ProjectID
	taskURL := s.config.BuildTaskURL(info) + shared.JSONExt
	if base.IsLoggingEnabled() {
		fmt.Printf("notyfying: %v -> %v\n", job.URL, taskURL)
	}
	return s.fs.Move(ctx, job.URL, taskURL, option.NewObjectKind(true))
}

func (s *service) dispatchBatchEvents(ctx context.Context, response *contract.Response, projectObjects *project.Events) (err error) {
	objects := projectObjects.Items
	perf := projectObjects.Performance
	if len(objects) == 0 {
		return nil
	}
	response.BatchCount = len(objects)
	for _, obj := range objects {
		if obj.IsDir() || path.Ext(obj.Name()) != shared.WindowExt {
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
		if time.Now().After(dueTime.Add(shared.StorageListVisibilityDelay * time.Millisecond)) {
			if !s.canNotify(shared.ActionLoad, perf) {
				continue
			}
			perf.Metric(shared.RunningState).BatchJobs++
			perf.Metric(shared.RunningState).LoadJobs++
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
	if strings.HasPrefix(encoded, shared.TempProjectPrefix) {
		if index := strings.Index(encoded, "/"); index != -1 {
			encoded = string(encoded[index+1:])
		}
	}
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
