package tail

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	astorage "github.com/viant/afs/storage"
	"github.com/viant/afs/sync"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/auth"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/base/job"
	"github.com/viant/bqtail/schema"
	sbatch "github.com/viant/bqtail/service/batch"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/service/http"
	"github.com/viant/bqtail/service/pubsub"
	"github.com/viant/bqtail/service/secret"
	"github.com/viant/bqtail/service/slack"
	"github.com/viant/bqtail/service/storage"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/stage/activity"
	"github.com/viant/bqtail/stage/load"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/contract"
	"github.com/viant/bqtail/tail/status"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	goption "google.golang.org/api/option"
	"strings"
	"time"
)

//Service represents a tail service
type Service interface {
	//Tails appends data from source URL to matched BigQuery table
	Tail(ctx context.Context, request *contract.Request) *contract.Response
}

type service struct {
	task.Registry
	bq     bq.Service
	batch  batch.Service
	fs     afs.Service
	cfs    afs.Service
	config *Config
}

func (s *service) Init(ctx context.Context) error {
	err := s.config.Init(ctx, s.cfs)
	if err != nil {
		return err
	}
	slackService := slack.New(s.config.Region, s.config.ProjectID, s.fs, secret.New(), s.config.SlackCredentials)
	slack.InitRegistry(s.Registry, slackService)

	options := []goption.ClientOption{goption.WithScopes(auth.Scopes...)}
	if client, _ := auth.DefaultHTTPClientProvider(ctx, auth.Scopes); client != nil {
		options = append(options, goption.WithHTTPClient(client))
	}

	pubsubService, err := pubsub.New(ctx, s.config.ProjectID, options...)
	if err == nil {
		pubsub.InitRegistry(s.Registry, pubsubService)
	} else {
		shared.LogF("failed to create pubsub service: %v", err)
	}

	bqService, err := bigquery.NewService(ctx, options...)
	if err != nil {
		return err
	}
	s.bq = bq.New(bqService, s.Registry, s.config.ProjectID, s.fs, s.config.Config)
	s.batch = batch.New(s.config.TaskURL, s.fs)
	bq.InitRegistry(s.Registry, s.bq)
	http.InitRegistry(s.Registry, http.New())
	sbatch.InitRegistry(s.Registry, sbatch.New(s.fs, s.Registry))
	storage.InitRegistry(s.Registry, storage.New(s.fs))
	return err
}

func (s *service) Tail(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	response.TriggerURL = request.SourceURL

	var err error
	defer s.OnDone(ctx, request, response)
	if request.HasURLPrefix(s.config.LoadProcessPrefix) {
		err = s.runLoadProcess(ctx, request, response)

	} else if request.HasURLPrefix(s.config.PostJobPrefix) {
		err = s.runPostLoadActions(ctx, request, response)
	} else if request.HasURLPrefix(s.config.BatchPrefix) {
		err = s.runBatch(ctx, request, response)
	} else {
		response.IsDataFile = true
		err = s.tail(ctx, request, response)
	}

	if err != nil {
		response.SetIfError(err)
	}
	return response
}

func (s *service) tail(ctx context.Context, request *contract.Request, response *contract.Response) error {
	response.Retriable = true
	if err := s.config.ReloadIfNeeded(ctx, s.cfs); err != nil {
		return err
	}
	rule := s.matchSourceWithRule(response, request)
	if rule == nil {
		return nil
	}
	source, err := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
	if err != nil {
		response.NotFoundError = err.Error()
		response.Status = shared.StatusNotFound
		return nil
	}
	process, err := s.newProcess(ctx, source, rule, request, response)
	if err != nil {
		return err
	}
	var job *load.Job
	if rule.Batch != nil {
		job, err = s.tailInBatch(ctx, process, rule, response)
	} else {
		_, err = s.tailIndividually(ctx, process, rule, response)
		return err
	}
	if !job.Recoverable() {
		return err
	}
	return s.tryRecover(ctx, job, response)
}

func (s *service) OnDone(ctx context.Context, request *contract.Request, response *contract.Response) {
	response.ListOpCount = gs.GetListCounter(true)
	response.StorageRetries = gs.GetRetryCodes(true)
	response.SetTimeTaken(response.Started)
	if response.Error != "" {
		s.handlerProcessError(ctx, request, response)
	}

	if response.Retriable {
		if response.Error != "" {
			response.RetryError = response.Error
		}
		return
	}
	if response.IsDataFile {
		return
	}
	if e := s.fs.Delete(ctx, request.SourceURL, option.NewObjectKind(true)); e != nil && response.NotFoundError == "" {
		response.NotFoundError = fmt.Sprintf("failed to delete: %v, %v", request.SourceURL, e)
	}
}

func (s *service) handlerProcessError(ctx context.Context, request *contract.Request, response *contract.Response) {
	info := response.Process
	if info == nil {
		return
	}
	err := errors.New(response.Error)

	//if storage event is duplicated, you some asset being already removed, that said do not clear table no found error
	if base.IsNotFoundError(err) && !strings.Contains(err.Error(), base.TableFragment) {
		response.NotFoundError = err.Error()
		response.Retriable = false
		return
	}

	//Dump response to error URL
	if data, e := json.Marshal(response); e == nil {
		errorResponseURL := url.Join(s.config.ErrorURL, info.DestTable, fmt.Sprintf("%v%v", request.EventID, shared.ResponseErrorExt))
		if e := s.fs.Upload(ctx, errorResponseURL, file.DefaultFileOsMode, bytes.NewReader(data)); e != nil {
			response.UploadError = e.Error()
		}
	}

	//Check number of the error per event
	errorCounterURL := url.Join(s.config.JournalURL, shared.RetryCounterSubpath, info.EventID+shared.CounterExt)
	canRetry := s.canRetryEvent(ctx, errorCounterURL, response)
	if !canRetry {
		//if can not retry move process to done, and error location
		s.failProcess(ctx, info, request)
		//move current source file to retry location
		s.moveToRetryLocation(response, request, ctx)
		response.Retriable = true
		response.Error = ""
		response.RetryError = response.Error
		response.Status = shared.StatusOK
		return
	}

	//In case you can still retry, if retriable wait and then fail CF
	if base.IsRetryError(err) {
		//Put extra sleep otherwise retry may kick in immediately and service may no be back on
		time.Sleep(3 * time.Second)
		response.Retriable = true
		return
	}

	//Replay the whole load process - some individual BigQuery job can not be recovered
	if base.IsInternalError(err) || base.IsBackendError(err) {
		//Put extra sleep otherwise retry may kick in immediately and service may no be back on
		time.Sleep(3 * time.Second)
		if exists, _ := s.fs.Exists(ctx, info.ProcessURL); exists {
			err := s.restartProcess(ctx, info, request, response)
			response.Retriable = err != nil
			return
		}
	}
}

//failProcess  copy a a failed process to error location, and moves it to done location
func (s *service) failProcess(ctx context.Context, info *stage.Process, request *contract.Request) {
	processErrorURL := url.Join(s.config.ErrorURL, "proc", info.DestTable, fmt.Sprintf("%v%v", request.EventID, shared.ProcessExt))
	_ = s.fs.Copy(ctx, info.ProcessURL, processErrorURL)
	doneURL := s.config.DoneLoadURL(info)
	_ = s.fs.Move(ctx, info.ProcessURL, doneURL)
}

func (s *service) moveToRetryLocation(response *contract.Response, request *contract.Request, ctx context.Context) {
	location := url.Path(request.SourceURL)
	retryDataURL := url.Join(s.config.JournalURL, shared.RetryDataSubpath, request.EventID, location)
	if err := s.fs.Move(ctx, request.SourceURL, retryDataURL); err != nil {
		response.MoveError = err.Error()
	}
	errorURL := url.Join(s.config.JournalURL, shared.RetryCounterSubpath, request.EventID+shared.ErrorExt)
	_ = s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(response.Error))
}

func (s *service) canRetryEvent(ctx context.Context, errorCounterURL string, response *contract.Response) bool {
	counter := sync.NewCounter(errorCounterURL, s.fs)
	count, err := counter.Increment(ctx)
	if err != nil {
		response.CounterError = err.Error()
	}
	return count < s.config.MaxRetries
}

func (s *service) newProcess(ctx context.Context, source astorage.Object, rule *config.Rule, request *contract.Request, response *contract.Response) (*stage.Process, error) {
	result := stage.NewProcess(request.EventID, stage.NewSource(source.URL(), source.ModTime()), rule.Info.URL, rule.Async)
	var err error
	if result.DestTable, err = rule.Dest.ExpandTable(rule.Dest.Table, result.Source); err != nil {
		return nil, errors.Wrapf(err, "failed to expand table :%v", rule.Dest.Table)
	}
	result.ProcessURL = s.config.BuildLoadURL(result)
	result.DoneProcessURL = s.config.DoneLoadURL(result)
	result.FailedURL = url.Join(s.config.JournalURL, "failed")
	result.ProjectID = s.selectProjectID(ctx, rule, response)
	result.Params, err = rule.Dest.Params(result.Source.URL)
	if shared.IsDebugLoggingLevel() {
		shared.LogF("process: ")
		shared.LogLn(result)
	}
	return result, err
}

func (s *service) submitJob(ctx context.Context, job *load.Job, response *contract.Response) (*load.Job, error) {
	if len(job.Load.SourceUris) == 0 {
		return nil, errors.Errorf("sourceUris was empty")
	}
	loadRequest, action := job.NewLoadRequest()
	if err := job.Persist(ctx, s.fs); err != nil {
		response.UploadError = err.Error()
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogF("loadRequest: ")
		shared.LogLn(loadRequest)
	}
	bqJob, err := s.bq.Load(ctx, loadRequest, action)
	if bqJob != nil {
		job.JobStatus = bqJob.Status
		job.Statistics = bqJob.Statistics
		response.JobRef = bqJob.JobReference
		if err == nil {
			err = base.JobError(bqJob)
		}
	}
	job.BqJob = bqJob
	return job, err
}

//runLoadProcess this method allows rerun Activity/Done job as long original data files are present
func (s *service) runLoadProcess(ctx context.Context, request *contract.Request, response *contract.Response) error {
	process := &stage.Process{ProcessURL: request.SourceURL}
	processJob, err := load.NewJobFromURL(ctx, nil, process.ProcessURL, s.fs)
	if err != nil {
		response.NotFoundError = err.Error()
		return nil
	}
	processJob.EventID = request.EventID
	if processJob.RuleURL == "" {
		return errors.Errorf("rule URL was empty: %+v", processJob)
	}
	if processJob.Rule = s.config.Rule(ctx, processJob.RuleURL); processJob.Rule == nil {
		return errors.Errorf("failed to lookup rule: '%v'", processJob.RuleURL)
	}
	if shared.IsInfoLoggingLevel() {
		shared.LogF("\nreplaying load process %v ...\n", process.EventID)
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogLn(processJob)
	}
	_, err = s.submitJob(ctx, processJob, response)
	return err
}

func (s *service) tailIndividually(ctx context.Context, process *stage.Process, rule *config.Rule, response *contract.Response) (*load.Job, error) {
	job, err := load.NewJob(rule, process, nil, nil)
	if err != nil {
		return nil, err
	}
	err = job.Init(ctx, s.bq)
	if err != nil {
		return nil, err
	}
	return s.submitJob(ctx, job, response)
}

func (s *service) selectProjectID(ctx context.Context, rule *config.Rule, response *contract.Response) string {
	projectID := s.config.ProjectID
	if rule.Dest.Transient != nil {
		projectPerformance, err := LoadProjectPerformance(ctx, s.fs, &s.config.Config)
		if err != nil {
			response.DownloadError = err.Error()
		}
		projectID = rule.Dest.Transient.JobProjectID(projectPerformance)
	}
	return projectID
}

func (s *service) tailInBatch(ctx context.Context, process *stage.Process, rule *config.Rule, response *contract.Response) (*load.Job, error) {
	response.Batched = true
	batchWindow, err := s.batch.TryAcquireWindow(ctx, process, rule)
	if batchWindow == nil || err != nil {
		if err != nil {
			return nil, errors.Wrapf(err, "failed to acquire batch window")
		}
	}
	if batchWindow.OwnerEventID != "" {
		response.BatchingEventID = batchWindow.OwnerEventID
		response.WindowURL = batchWindow.WindowURL
	}
	if batchWindow.Window == nil {
		return nil, nil
	}
	response.Window = batchWindow.Window
	if rule.Async {
		return nil, nil
	}
	return s.runInBatch(ctx, rule, batchWindow.Window, response)
}

func (s *service) runPostLoadActions(ctx context.Context, request *contract.Request, response *contract.Response) error {
	action, err := task.NewActionFromURL(ctx, s.fs, request.SourceURL)
	if err != nil {
		object, _ := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
		if object == nil {
			response.NotFoundError = err.Error()
			return nil
		}
		return err
	}
	response.Process = &action.Meta.Process
	action.Meta.Region = action.Job.JobReference.Location
	action.Meta.ProjectID = action.Job.JobReference.ProjectId
	projectID := action.Meta.GetOrSetProject(s.config.ProjectID)
	bqJob, err := s.bq.GetJob(ctx, action.Job.JobReference.Location, projectID, action.Job.JobReference.JobId)
	if bqErr := base.JobError(bqJob); bqErr != nil {
		errorURL := url.Join(s.config.ErrorURL, action.Meta.DestTable, fmt.Sprintf("%v%v", action.Meta.EventID, shared.ErrorExt))
		_ = s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(bqErr.Error()))
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogLn(request)
		if bqJob != nil {
			shared.LogLn(bqJob.Status)
		}
	}
	if err != nil {
		response.Retriable = base.IsRetryError(err)
		return errors.Wrapf(err, "failed to fetch aJob %v,", action.Job.JobReference.JobId)
	}
	if err := s.logJobInfo(ctx, bqJob, action); err != nil {
		response.UploadError = fmt.Sprintf("failed to log aJob info: %v", err.Error())
	}
	bqJobError := base.JobError(bqJob)
	if bqJobError != nil && bqJob.Configuration != nil && bqJob.Configuration.Load != nil {
		if shared.IsDebugLoggingLevel() {
			shared.LogF("load error - reloading ...\n")
		}
		rule := s.config.Rule(ctx, action.Meta.RuleURL)
		processJob, err := load.NewJobFromURL(ctx, rule, action.Meta.Process.ProcessURL, s.fs)
		if err != nil {
			return bqJobError
		}
		processJob.BqJob = bqJob
		if !processJob.Recoverable() {
			return err
		}
		return s.tryRecover(ctx, processJob, response)
	}
	if action.Meta != nil {
		action.Meta.Step *= 100
	}
	if err := action.Init(ctx, s.cfs); err != nil {
		return err
	}

	//try retry copy/query/extract jobs
	if base.IsRetryError(bqJobError) || base.IsInternalError(bqJobError) {
		errorCounterURL := url.Join(s.config.JournalURL, shared.RetryCounterSubpath, action.Meta.EventID+shared.CounterExt)
		if canRetry := s.canRetryEvent(ctx, errorCounterURL, response); canRetry {
			response.Retriable = false
			_, err = task.Run(ctx, s.Registry, action)
			return err
		}
	}

	bqjob := base.Job(*bqJob)
	toRun := action.ToRun(bqJobError, &bqjob)
	retriable, err := task.RunAll(ctx, s.Registry, toRun)
	if retriable {
		response.Retriable = true
	}
	if err != nil {
		return err
	}
	return bqJobError
}

func (s *service) runBatch(ctx context.Context, request *contract.Request, response *contract.Response) error {
	window, err := batch.GetWindow(ctx, request.SourceURL, s.fs)
	if err != nil {
		object, _ := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
		if object == nil {
			response.NotFoundError = err.Error()
			return nil
		}
		if object.Size() == 0 {
			return err
		}
		if window, err = batch.GetWindow(ctx, request.SourceURL, s.fs); err != nil {
			return err
		}
	}
	request.EventID = window.EventID
	rule := s.config.Rule(ctx, window.RuleURL)
	loadJob, batchErr := s.runInBatch(ctx, rule, window, response)
	if !loadJob.Recoverable() {
		if batchErr != nil {
			response.Retriable = base.IsRetryError(batchErr)
		}
		return batchErr
	}
	return s.tryRecover(ctx, loadJob, response)
}

func (s *service) runInBatch(ctx context.Context, rule *config.Rule, window *batch.Window, response *contract.Response) (*load.Job, error) {
	response.Window = window
	response.BatchRunner = true
	if rule == nil {
		return nil, fmt.Errorf("rule was empty for %v", window.RuleURL)
	}
	batchingDistributionDelay := time.Duration(getRandom(shared.StorageListVisibilityDelayMs, rule.Batch.MaxDelayMs(shared.StorageListVisibilityDelayMs))) * time.Millisecond
	remainingDuration := window.End.Sub(time.Now().UTC()) + batchingDistributionDelay
	if remainingDuration < 0 && window.IsSyncMode() { //intendent for client sync mode
		remainingDuration = time.Duration(shared.StorageListVisibilityDelayMs) * time.Millisecond
	}
	if shared.IsInfoLoggingLevel() {
		shared.LogF("[%v] starting batch window: %s\n", window.DestTable, rule.Batch.Window.Duration)
	}
	if remainingDuration > 0 {
		time.Sleep(remainingDuration)
	}
	err := s.batch.MatchWindowDataURLs(ctx, rule, window)
	if err != nil || len(window.URIs) == 0 {
		return nil, err
	}
	_ = s.logBatchInfo(ctx, window)
	group, err := s.loadGroup(ctx, rule, window)
	if err != nil {
		return nil, err
	}
	loadJob, jobErr := load.NewJob(rule, window.Process, window, group)
	if jobErr != nil {
		return nil, jobErr
	}
	err = loadJob.Init(ctx, s.bq)
	if err != nil {
		return nil, err
	}
	loadJob, err = s.submitJob(ctx, loadJob, response)
	return loadJob, err
}

func (s *service) loadGroup(ctx context.Context, rule *config.Rule, window *batch.Window) (*batch.Group, error) {
	if rule.Batch.Group == nil {
		return nil, nil
	}
	return s.batch.AcquireGroup(ctx, window.Process, rule)
}

func (s *service) tryRecover(ctx context.Context, job *load.Job, response *contract.Response) error {
	err := base.JobError(job.BqJob)
	if err == nil {
		return err
	}
	configuration := job.BqJob.Configuration
	response.Process = job.Process

	if configuration.Load == nil || len(configuration.Load.SourceUris) == 0 {
		err := base.JobError(job.BqJob)
		response.Retriable = base.IsRetryError(err)
		_ = job.MoveToFailed(ctx, s.fs)
		return err
	}
	response.LoadError = base.JobError(job.BqJob).Error()
	uris := status.NewURIs()
	uris.Classify(ctx, s.fs, job.BqJob)

	if len(uris.InvalidSchema) > 0 {
		if job.Rule.Dest.AllowFieldAddition {
			uris.Valid = append(uris.Valid, uris.InvalidSchema...)
			uris.InvalidSchema = []string{}
			if err := s.addMissingFields(ctx, job, uris); err != nil {
				return err
			}
		}
	}

	corruptedFileURL, invalidSchemaURL := s.getDataErrorsURLs(job.Rule)
	if shared.IsInfoLoggingLevel() {
		if len(uris.Corrupted) > 0 || len(uris.InvalidSchema) > 0 {
			shared.LogF("[%v] excluding corrupted: %v, incompatible schema: %v file(s)\n", job.DestTable, len(uris.Corrupted), len(uris.InvalidSchema))
		}
	}

	if len(uris.InvalidSchema) == 0 && len(uris.Corrupted) == 0 && len(uris.Missing) == 0 && len(uris.MissingFields) == 0 {
		return base.JobError(job.BqJob)
	}

	if err := s.moveAssets(ctx, uris.Corrupted, corruptedFileURL); err != nil {
		err = errors.Wrapf(err, "failed to move %v to %v", response.Corrupted, corruptedFileURL)
		response.MoveError = err.Error()
	}

	if err := s.moveAssets(ctx, uris.InvalidSchema, invalidSchemaURL); err != nil {
		err = errors.Wrapf(err, "failed to move %v to %v", response.InvalidSchema, invalidSchemaURL)
		response.MoveError = err.Error()
	}

	if len(uris.Valid) == 0 {
		response.Retriable = false
		return nil
	}
	response.Status = shared.StatusOK
	response.Error = ""
	job.Load.SourceUris = uris.Valid
	loadRequest, action := job.NewLoadRequest()
	meta := activity.Parse(job.BqJob.JobReference.JobId)
	action.Meta.Step = meta.Step + 1
	reloadCount := meta.Step - 1
	if shared.IsDebugLoggingLevel() {
		shared.LogF("reload attempt: %v\n", reloadCount)
		shared.LogLn(meta)
	}
	if reloadCount > job.Rule.MaxReloadAttempts() {
		return base.JobError(job.BqJob)
	}
	action.Meta = action.Meta.Wrap(shared.ActionReload)
	loadJob, err := s.bq.Load(ctx, loadRequest, action)
	if err == nil {
		err = base.JobError(loadJob)
	}
	if err != nil && loadJob != nil {
		job.BqJob = loadJob
		return s.tryRecover(ctx, job, response)
	}
	return err
}

func (s *service) getDataErrorsURLs(rule *config.Rule) (string, string) {
	corruptedFileURL := s.config.CorruptedFileURL
	invalidSchemaURL := s.config.InvalidSchemaURL
	if rule != nil {
		if rule.CorruptedFileURL != "" {
			corruptedFileURL = rule.CorruptedFileURL
		}
		if rule.InvalidSchemaURL != "" {
			invalidSchemaURL = rule.InvalidSchemaURL
		}
	}
	return corruptedFileURL, invalidSchemaURL
}

func (s *service) moveAssets(ctx context.Context, URLs []string, baseDestURL string) error {
	var err error
	if len(URLs) == 0 {
		return nil
	}
	for _, sourceURL := range URLs {
		_, URLPath := url.Base(sourceURL, "")
		destURL := url.Join(baseDestURL, URLPath)
		e := s.fs.Move(ctx, sourceURL, destURL)

		if shared.IsDebugLoggingLevel() {
			shared.LogLn(fmt.Sprintf("moving: %v %v, %v\n", sourceURL, destURL, err))
		}
		if e != nil {
			if exists, _ := s.fs.Exists(ctx, sourceURL, option.NewObjectKind(true)); !exists {
				continue
			}
			err = e
		}
	}
	return err
}

//restartProcess restart process, start ingestion process from scratch using original process execution plan
func (s *service) restartProcess(ctx context.Context, process *stage.Process, request *contract.Request, response *contract.Response) error {
	if !process.Async {
		resp := contract.NewResponse(request.EventID)
		err := s.runLoadProcess(ctx, &contract.Request{
			SourceURL: process.ProcessURL,
			EventID:   fmt.Sprintf("%10d", int32(time.Now().Unix())),
		}, resp)
		if err == nil {
			response.RetryError = response.Error
			response.Error = resp.Error
			response.Status = resp.Status
		}
		return nil
	}
	processURL := process.ProcessURL
	bucket := url.Host(request.SourceURL)
	_, name := url.Split(processURL, gs.Scheme)
	loadJobURL := fmt.Sprintf("gs://%v/%v/%v", bucket, s.config.LoadProcessPrefix, name)
	return s.fs.Copy(ctx, processURL, loadJobURL)
}

func (s *service) logBatchInfo(ctx context.Context, window *batch.Window) error {
	if s.config.BqBatchInfoPath == "" {
		return nil
	}
	URL := url.Join(fmt.Sprintf("gs://%v/", s.config.TriggerBucket), s.config.BqBatchInfoPath, window.EventID+shared.JSONExt)
	data, err := json.Marshal(window)
	if err != nil {
		return err
	}
	window.Resources = nil
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *service) logJobInfo(ctx context.Context, bqjob *bigquery.Job, action *task.Action) error {
	if s.config.BqJobInfoPath == "" {
		return nil
	}
	info := job.NewInfo(bqjob)
	if action != nil && action.Meta != nil {
		info.EventID = action.Meta.EventID
		info.TempTable = action.Meta.TempTable
		info.RuleURL = action.Meta.RuleURL
	}
	URL := url.Join(fmt.Sprintf("gs://%v/", s.config.TriggerBucket), s.config.BqJobInfoPath, bqjob.JobReference.JobId+shared.JSONExt)
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *service) addMissingFields(ctx context.Context, job *load.Job, uris *status.URIs) error {

	for _, field := range uris.MissingFields {
		if err := field.AdjustType(ctx, s.fs); err != nil {
			return err
		}
		if shared.IsInfoLoggingLevel() {
			shared.LogF("Adding field: %v %v\n", field.Name, field.Type)
		}
	}

	if job.TempTable != "" {
		if err := s.PatchedTable(ctx, uris.MissingFields, job.TempTable); err != nil {
			return err
		}
	}
	if job.DestTable != "" {
		if err := s.PatchedTable(ctx, uris.MissingFields, job.DestTable); err != nil {
			return err
		}
	}
	if job.Rule.Dest.Schema.Template != "" {
		if err := s.PatchedTable(ctx, uris.MissingFields, job.Rule.Dest.Schema.Template); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) PatchedTable(ctx context.Context, fields []*status.Field, tableName string) error {
	tableRef, _ := base.NewTableReference(tableName)
	table, err := s.bq.Table(ctx, tableRef)
	if err != nil {
		return err
	}

	schemaFields := table.Schema.Fields
	for _, field := range fields {
		if len(field.Fields) == 0 {
			return errors.Errorf("failed to detect schema for %v", field.Name)
		}
		schemaFields = schema.MergeFields(schemaFields, field.Fields)
	}

	table.Schema.Fields = schemaFields
	table.ExpirationTime = 0
	_, err = s.bq.Patch(ctx, &bq.PatchRequest{
		Template:      "",
		Table:         tableName,
		TemplateTable: table,
	})
	return err
}

//New creates a new service
func New(ctx context.Context, config *Config) (Service, error) {
	srv := &service{
		config:   config,
		fs:       afs.New(),
		cfs:      cache.Singleton(config.URL),
		Registry: task.NewRegistry(),
	}
	return srv, srv.Init(ctx)
}
