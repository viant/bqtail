package tail

import (
	"bqtail/base"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/stage"
	"bqtail/tail/batch"
	"bqtail/tail/config"
	"bqtail/tail/contract"
	"bqtail/tail/sql"
	"bqtail/tail/status"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/option"
	store "github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
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
	bqService, err := bigquery.NewService(ctx)
	if err != nil {
		return err
	}
	s.bq = bq.New(bqService, s.Registry, s.config.ProjectID, s.fs, s.config.Config)
	s.batch = batch.New(s.config.BatchURL, s.fs)
	bq.InitRegistry(s.Registry, s.bq)
	storage.InitRegistry(s.Registry, storage.New(s.fs))
	return err
}

func (s *service) OnDone(ctx context.Context, request *contract.Request, response *contract.Response) {
	response.ListOpCount = gs.GetListCounter(true)
	response.StorageRetries = gs.GetRetryCodes(true)
	response.SetTimeTaken(response.Started)
	if response.Retriable {
		return
	}
	if e := s.fs.Delete(ctx, request.SourceURL, option.NewObjectKind(true)); e != nil && response.NotFoundError == "" {
		response.NotFoundError = fmt.Sprintf("failed to delete: %v, %v", request.SourceURL, e)
	}
}

func (s *service) Tail(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	response.TriggerURL = request.SourceURL

	defer s.OnDone(ctx, request, response)
	var err error
	if request.HasURLPrefix(s.config.LoadJobPrefix) {
		err = s.runLoadAction(ctx, request, response)

	} else if request.HasURLPrefix(s.config.BqJobPrefix) {
		err = s.runPostLoadActions(ctx, request, response)

	} else if request.HasURLPrefix(s.config.BatchPrefix) {
		err = s.runBatch(ctx, request, response)

	} else {
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
	response.RuleCount = len(s.config.Rules)
	var rule *config.Rule
	matched := s.config.Match(request.SourceURL)
	switch len(matched) {
	case 0:
	case 1:
		rule = matched[0]
	default:
		JSON, _ := json.Marshal(matched)
		return errors.Errorf("multi rule match currently not supported: %s", JSON)
	}
	if rule == nil {
		response.Status = base.StatusNoMatch
		return nil
	}
	response.Rule = rule
	response.Matched = true
	response.MatchedURL = request.SourceURL
	if rule.Disabled {
		response.Status = base.StatusDisabled
		return nil
	}
	source, err := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
	if err != nil {
		response.NotFoundError = err.Error()
		response.Status = base.StatusNotFound
		return nil
	}
	var job *Job
	if rule.Batch != nil {
		job, err = s.tailInBatch(ctx, source, rule, request, response)
	} else {
		_, err = s.tailIndividually(ctx, source, rule, request, response)
		return err
	}
	if err == nil || job == nil {
		return err
	}
	return s.tryRecover(ctx, request, job.Actions, job.Job, response)
}

func (s *service) buildLoadRequest(ctx context.Context, job *Job, rule *config.Rule) (*bq.LoadRequest, error) {
	dest := rule.Dest
	sourceURL := job.Load.SourceUris[0]
	tableReference, err := dest.TableReference(job.SourceCreated, sourceURL)
	if err != nil {
		return nil, err
	}
	if err = s.updateSchemaIfNeeded(ctx, dest, tableReference, job); err != nil {
		return nil, err
	}
	result := &bq.LoadRequest{
		Append:               rule.IsAppend(),
		JobConfigurationLoad: job.Load,
	}
	if dest.TransientDataset != "" {
		tableReference.ProjectId = s.config.ProjectID
		tableReference.DatasetId = dest.TransientDataset
		tableReference.TableId = base.TableID(tableReference.TableId) + "_" + job.EventID
		result.WriteDisposition = "WRITE_TRUNCATE"
	} else {
		if rule.IsAppend() {
			result.WriteDisposition = "WRITE_APPEND"
		} else {
			result.WriteDisposition = "WRITE_TRUNCATE"
		}
	}
	result.DestinationTable = tableReference
	return result, nil
}

func (s *service) submitJob(ctx context.Context, job *Job, rule *config.Rule, response *contract.Response) (*Job, error) {
	if len(job.Load.SourceUris) == 0 {
		return nil, errors.Errorf("sourceUris was empty")
	}
	load, err := s.buildLoadRequest(ctx, job, rule)
	if err != nil {
		return nil, err
	}

	actions := rule.Actions.Expand(&base.Expandable{SourceURLs: job.Load.SourceUris})
	info := stage.New(job.GetSourceURI(), job.Dest(), job.EventID, "load", job.IDSuffix(), rule.Async, 0, rule.Info.URL)
	actions.SetInfo(info)
	if err = appendBatchAction(job.Window, actions); err == nil {
		info := job.Info()
		actions, err = s.addTransientDatasetActions(ctx, *info, job, rule, actions)
	}
	if err != nil {
		return nil, err
	}
	load.Actions = *actions
	if rule.Dest.HasSplit() {
		if err = s.updateTempTableScheme(ctx, load.JobConfigurationLoad, rule); err != nil {
			return nil, errors.Wrapf(err, "failed to upload load schema")
		}
	}

	activeURL := s.config.BuildActiveLoadURL(job.Info())
	doneURL := s.config.BuildDoneLoadURL(job.Info())

	s.appendLoadJobFinalActions(activeURL, doneURL, load)
	if e := s.createLoadAction(ctx, activeURL, load); e != nil {
		return nil, errors.Wrapf(err, "failed to create load job actions: %v", activeURL)
	}
	bqJob, err := s.bq.Load(ctx, load)
	if bqJob != nil {
		job.JobStatus = bqJob.Status
		job.Statistics = bqJob.Statistics
		actions.Job = bqJob
		response.JobRef = bqJob.JobReference
		if err == nil {
			err = base.JobError(bqJob)
		}
	}
	job.Actions = actions
	return job, err
}

//appendLoadJobFinalActions append track action
func (s *service) appendLoadJobFinalActions(activeURL, doneURL string, load *bq.LoadRequest) {
	moveRequest := storage.MoveRequest{SourceURL: activeURL, DestURL: doneURL, IsDestAbsoluteURL: true}
	moveAction, _ := task.NewAction("move", moveRequest)
	load.Actions.AddOnSuccess(moveAction)
}

func (s *service) createLoadAction(ctx context.Context, URL string, loadJob *bq.LoadRequest) error {
	loadTrace, err := task.NewAction("load", loadJob)
	if err != nil {
		return err
	}
	actions := []*task.Action{
		loadTrace,
	}
	JSON, err := json.Marshal(actions)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal: %v", loadTrace)
	}
	return s.fs.Upload(ctx, URL, 0644, bytes.NewReader(JSON))
}

func appendBatchAction(window *batch.Window, actions *task.Actions) error {
	if window == nil {
		return nil
	}
	URLsToDelete := make([]string, 0)
	URLsToDelete = append(URLsToDelete, window.URL)
	deleteReq := storage.DeleteRequest{URLs: URLsToDelete}
	deleteAction, err := task.NewAction("delete", deleteReq)
	if err != nil {
		return err
	}
	actions.AddOnSuccess(deleteAction)

	return nil
}

func (s *service) addTransientDatasetActions(ctx context.Context, parent stage.Info, job *Job, rule *config.Rule, actions *task.Actions) (*task.Actions, error) {
	if rule.Dest.TransientDataset == "" {
		return actions, nil
	}
	job.Load.WriteDisposition = "WRITE_TRUNCATE"
	var result = task.NewActions(actions.Async, parent, nil, nil)
	var onFailureAction *task.Actions
	if actions != nil {
		result.SourceURI = job.GetSourceURI()
		onFailureAction = actions.CloneOnFailure()
		result.AddOnFailure(actions.OnFailure...)
	}
	tableID := job.Load.DestinationTable.DatasetId + "." + job.Load.DestinationTable.TableId
	dropAction, err := task.NewAction("drop", bq.NewDropRequest(tableID, onFailureAction))
	if err != nil {
		return nil, err
	}
	actions.AddOnSuccess(dropAction)
	selectAll := sql.BuildSelect(job.Load.DestinationTable, job.Load.Schema, rule.Dest.UniqueColumns, rule.Dest.Transform)
	if rule.Dest.HasSplit() {
		return result, s.addSplitActions(ctx, selectAll, parent, job, rule, result, actions)
	}
	selectAll = strings.Replace(selectAll, "$WHERE", "", 1)
	destTable, _ := rule.Dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])

	partition := base.TablePartition(destTable.TableId)

	if len(rule.Dest.UniqueColumns) > 0 || partition != "" {
		query := bq.NewQueryRequest(selectAll, destTable, actions)
		query.Append = rule.IsAppend()
		queryAction, err := task.NewAction("query", query)
		if err != nil {
			return nil, err
		}
		result.AddOnSuccess(queryAction)
	} else {
		source := base.EncodeTableReference(job.Load.DestinationTable)
		dest := base.EncodeTableReference(destTable)
		copyRequest := bq.NewCopyRequest(source, dest, actions)
		copyRequest.Append = rule.IsAppend()
		copyAction, err := task.NewAction("copy", copyRequest)
		if err != nil {
			return nil, err
		}
		result.AddOnSuccess(copyAction)
	}
	return result, nil
}

func getColumn(fields []*bigquery.TableFieldSchema, column string) *bigquery.TableFieldSchema {
	column = strings.ToLower(column)
	if index := strings.Index(column, "."); index != -1 {
		parent := string(column[:index])
		for i := range fields {
			if parent == strings.ToLower(fields[i].Name) {
				return getColumn(fields[i].Fields, column[index+1:])
			}
		}
	}
	for i := range fields {
		if column == strings.ToLower(fields[i].Name) {
			return fields[i]
		}
	}
	return nil
}

func (s *service) updateTempTableScheme(ctx context.Context, job *bigquery.JobConfigurationLoad, rule *config.Rule) error {
	split := rule.Dest.Schema.Split
	if job.Schema == nil {
		return nil
	}
	if len(split.ClusterColumns) > 0 {
		if split.TimeColumn == "" {
			split.TimeColumn = "ts"
		}
		field := getColumn(job.Schema.Fields, split.TimeColumn)
		if field == nil {
			job.Schema.Fields = append(job.Schema.Fields, &bigquery.TableFieldSchema{
				Name: split.TimeColumn,
				Type: "TIMESTAMP",
			})
		}
		job.TimePartitioning = &bigquery.TimePartitioning{
			Field: split.TimeColumn,
			Type:  "DAY",
		}

		var clusterdColumn = make([]string, 0)
		for i, name := range split.ClusterColumns {
			if strings.Contains(split.ClusterColumns[i], ".") {
				column := getColumn(job.Schema.Fields, split.ClusterColumns[i])
				if column == nil {
					return errors.Errorf("failed to lookup cluster column: %v", name)
				}
				job.Schema.Fields = append(job.Schema.Fields, column)
				clusterdColumn = append(clusterdColumn, column.Name)
				continue
			}
			clusterdColumn = append(clusterdColumn, split.ClusterColumns[i])
		}

		job.Clustering = &bigquery.Clustering{
			Fields: clusterdColumn,
		}
	}
	return nil
}

func (s *service) addSplitActions(ctx context.Context, selectAll string, parent stage.Info, job *Job, rule *config.Rule, result, onDone *task.Actions) error {
	split := rule.Dest.Schema.Split
	next := onDone
	if next == nil {
		next = task.NewActions(rule.Async, parent, nil, nil)
	}
	for i := range split.Mapping {

		mapping := split.Mapping[i]
		destTable, _ := rule.Dest.CustomTableReference(mapping.Then, job.SourceCreated, job.Load.SourceUris[0])
		dest := strings.Replace(selectAll, "$WHERE", " WHERE  "+mapping.When+" ", 1)
		query := bq.NewQueryRequest(dest, destTable, next)
		query.Append = rule.IsAppend()
		queryAction, err := task.NewAction("query", query)
		if err != nil {
			return err
		}
		group := task.NewActions(rule.Async, parent, nil, nil)
		group.AddOnSuccess(queryAction)
		next = group
	}

	if len(split.ClusterColumns) > 0 {
		setColumns := []string{}
		for i, column := range split.ClusterColumns {
			if index := strings.LastIndex(split.ClusterColumns[i], "."); index != -1 {
				setColumns = append(setColumns, fmt.Sprintf("%v = %v ", string(column[index+1:]), column))
			}
		}
		if len(setColumns) > 0 {
			refTable := job.Load.DestinationTable
			destTable := fmt.Sprintf("`%v.%v.%v`", refTable.ProjectId, refTable.DatasetId, refTable.TableId)
			DML := fmt.Sprintf("UPDATE %v SET %v WHERE 1=1", destTable, strings.Join(setColumns, ","))

			query := bq.NewQueryRequest(DML, nil, next)
			query.Append = rule.IsAppend()
			queryAction, err := task.NewAction("query", query)
			if err != nil {
				return err
			}
			result.AddOnSuccess(queryAction)
		}
	} else {
		result.AddOnSuccess(next.OnSuccess...)
		result.AddOnSuccess(next.OnFailure...)
	}

	return nil
}

//runLoadAction this method allows rerun Activity/Done job as long original data files are present
func (s *service) runLoadAction(ctx context.Context, request *contract.Request, response *contract.Response) error {
	actions := []*task.Action{}
	reader, err := s.fs.DownloadWithURL(ctx, request.SourceURL)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()

	if err = json.NewDecoder(reader).Decode(&actions); err != nil {
		return errors.Wrapf(err, "unable decode load action: %v", request.SourceURL)
	}
	replacement := buildJobIDReplacementMap(request.EventID, actions)
	for _, action := range actions {
		action.Request = toolbox.ReplaceMapKeys(action.Request, replacement, true)
		toolbox.Dump(action.Request)
		if err = task.Run(ctx, s.Registry, action); err != nil {
			return err
		}
	}
	_, sourcePath := url.Base(request.SourceURL, "")
	journalURL := url.Join(s.config.JournalURL, sourcePath)
	if e := s.fs.Move(ctx, request.SourceURL, journalURL, option.NewObjectKind(true)); e != nil {
		response.NotFoundError = e.Error()
	}
	return err
}

func (s *service) tailIndividually(ctx context.Context, source store.Object, rule *config.Rule, request *contract.Request, response *contract.Response) (*Job, error) {
	object, err := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
	if err != nil {
		return nil, errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	job := &Job{
		Rule:          rule,
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: object.ModTime(),
		DestTable:     rule.DestTable(source.URL(), source.ModTime()),
	}
	if job.Load, err = rule.Dest.NewJobConfigurationLoad(time.Now(), request.SourceURL); err != nil {
		return nil, err
	}
	return s.submitJob(ctx, job, rule, response)
}

func (s *service) updateSchemaIfNeeded(ctx context.Context, dest *config.Destination, tableReference *bigquery.TableReference, job *Job) error {
	if dest.Schema.Table != nil {
		return nil
	}
	var err error
	var table *bigquery.Table

	if dest.Schema.Template != "" {
		templateReference, err := base.NewTableReference(dest.Schema.Template)
		if err != nil {
			return errors.Wrapf(err, "invalid schema.template table name: %v", dest.Schema.Template)
		}
		if table, err = s.bq.Table(ctx, templateReference); err != nil {
			return errors.Wrapf(err, "failed to get tempalte table: %v", templateReference)
		}
	} else if dest.TransientDataset != "" {
		if table, err = s.bq.Table(ctx, tableReference); err != nil {
			return err
		}
	}

	if table != nil {
		job.Load.Schema = table.Schema
		if job.Load.Schema == nil && dest.Schema.Autodetect {
			job.Load.Autodetect = dest.Schema.Autodetect
		}
		if table.TimePartitioning != nil {
			job.Load.TimePartitioning = table.TimePartitioning
			job.Load.TimePartitioning.RequirePartitionFilter = false
		}
		if table.RangePartitioning != nil {
			job.Load.RangePartitioning = table.RangePartitioning
		}
		if table.Clustering != nil {
			job.Load.Clustering = table.Clustering
		}
	}
	return nil
}

func (s *service) tailInBatch(ctx context.Context, source store.Object, rule *config.Rule, request *contract.Request, response *contract.Response) (*Job, error) {
	batchWindow, err := s.batch.TryAcquireWindow(ctx, request.EventID, source, rule)
	if batchWindow == nil || err != nil {
		if err != nil {
			return nil, errors.Wrapf(err, "failed to acquire batch window")
		}
	}
	if batchWindow.OwnerEventID != "" {
		response.BatchingEventID = batchWindow.OwnerEventID
	}
	if batchWindow.Window == nil {
		return nil, nil
	}

	response.Window = batchWindow.Window
	if rule.Async {
		return nil, nil
	}
	return s.runInBatch(ctx, batchWindow.Window, response)
}

func (s *service) runPostLoadActions(ctx context.Context, request *contract.Request, response *contract.Response) error {
	actions, err := task.NewActionFromURL(ctx, s.fs, request.SourceURL)
	if err != nil {
		object, _ := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
		if object == nil {
			response.NotFoundError = err.Error()
			return nil
		}
		if object.Size() == 0 {
			return err
		}
		if actions, err = task.NewActionFromURL(ctx, s.fs, request.SourceURL); err != nil {
			return err
		}
	}
	bqJob, err := s.bq.GetJob(ctx, s.config.ProjectID, actions.Job.JobReference.JobId)
	if err != nil {
		response.Retriable = true
		return errors.Wrapf(err, "failed to fetch job %v", actions.Job.JobReference.JobId)
	}
	job := base.Job(*bqJob)
	bqJobError := job.Error()
	if bqJobError != nil {
		if bqJobError = s.tryRecover(ctx, request, actions, bqJob, response); bqJobError == nil {
			return nil
		}
	}
	toRun := actions.ToRun(bqJobError, &job, s.config.AsyncTaskURL)
	if len(toRun) > 0 {
		for i := range toRun {
			if err = task.Run(ctx, s.Registry, toRun[i]); err != nil {
				return err
			}
		}
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
	job, err := s.runInBatch(ctx, window, response)
	if err == nil || job == nil {
		response.Retriable = true
		return err
	}
	return s.tryRecover(ctx, request, job.Actions, job.Job, response)
}

func (s *service) runInBatch(ctx context.Context, window *batch.Window, response *contract.Response) (*Job, error) {
	response.Window = window
	response.BatchRunner = true
	rule := s.config.Get(ctx, window.RuleURL, window.Filter)
	if rule == nil {
		return nil, fmt.Errorf("failed locaet rule for :%v, %v", window.RuleURL, window.Filter)
	}
	batchingDistributionDelay := time.Duration(getRandom(base.StorageListVisibilityDelay, rule.Batch.MaxDelayMs(base.StorageListVisibilityDelay))) * time.Millisecond
	remainingDuration := window.End.Sub(time.Now()) + batchingDistributionDelay
	if remainingDuration > 0 {
		time.Sleep(remainingDuration)
	}
	err := s.batch.MatchWindowDataURLs(ctx, rule, window)
	if err != nil || len(window.URIs) == 0 {
		return nil, err
	}
	job := &Job{
		Rule:      rule,
		Status:    base.StatusOK,
		EventID:   window.EventID,
		Window:    window,
		DestTable: rule.DestTable(window.URIs[0], window.SourceTime),
	}
	if job.Load, err = rule.Dest.NewJobConfigurationLoad(time.Now(), window.URIs...); err != nil {
		return nil, err
	}
	job, err = s.submitJob(ctx, job, rule, response)
	if err == nil && len(window.Locations) > 0 {
		for _, URL := range window.Locations {
			_ = s.fs.Delete(ctx, URL, option.NewObjectKind(true))
		}
	}
	return job, err
}

func (s *service) tryRecover(ctx context.Context, request *contract.Request, actions *task.Actions, job *bigquery.Job, response *contract.Response) error {
	configuration := actions.Job.Configuration
	if configuration.Load == nil || len(configuration.Load.SourceUris) <= 1 {
		return base.JobError(job)
	}
	uris := status.NewURIs()
	response.URIs = *uris
	uris.Classify(ctx, s.fs, job)
	if err := s.moveAssets(ctx, uris.Corrupted, s.config.CorruptedFileURL); err != nil {
		err = errors.Wrapf(err, "failed to move %v to %v", response.Corrupted, s.config.CorruptedFileURL)
		response.NotFoundError = err.Error()
	}
	if err := s.moveAssets(ctx, uris.InvalidSchema, s.config.InvalidSchemaURL); err != nil {
		err = errors.Wrapf(err, "failed to move %v to %v", response.Corrupted, s.config.InvalidSchemaURL)
		response.NotFoundError = err.Error()
	}
	if len(uris.Valid) == 0 {
		return base.JobError(job)
	}
	response.Status = base.StatusOK
	response.Error = ""
	load := &bq.LoadRequest{
		JobConfigurationLoad: configuration.Load,
	}
	load.SourceUris = uris.Valid
	if actions != nil {
		load.Actions = *actions
	}
	bqJob := bigquery.Job(*job)
	load.Job = &bqJob
	load.Info = *load.Wrap("reload")
	response.JobRef = bqJob.JobReference
	load.ProjectID = s.config.ProjectID
	loadJob, err := s.bq.Load(ctx, load)
	if err == nil {
		err = base.JobError(loadJob)
	}
	return err
}

func (s *service) moveAssets(ctx context.Context, URLs []string, baseDestURL string) error {
	var err error
	if len(URLs) == 0 {
		return nil
	}
	for _, URL := range URLs {
		_, URLPath := url.Base(URL, "")
		destURL := url.Join(baseDestURL, URLPath)
		if e := s.fs.Move(ctx, URL, destURL); e != nil {
			if exists, _ := s.fs.Exists(ctx, URL, option.NewObjectKind(true)); !exists {
				continue
			}
			err = e
		}
	}
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
