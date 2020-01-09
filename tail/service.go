package tail

import (
	"bqtail/base"
	"bqtail/service/bq"
	"bqtail/service/http"
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
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	store "github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"io/ioutil"
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
	s.batch = batch.New(s.fs)
	bq.InitRegistry(s.Registry, s.bq)
	http.InitRegistry(s.Registry, http.New())
	storage.InitRegistry(s.Registry, storage.New(s.fs))
	return err
}

func (s *service) OnDone(ctx context.Context, request *contract.Request, response *contract.Response) {
	response.ListOpCount = gs.GetListCounter(true)
	response.StorageRetries = gs.GetRetryCodes(true)
	response.SetTimeTaken(response.Started)

	errorCounterURL := url.Join(s.config.JournalURL, base.RetryCounterSubpath, request.EventID+base.CounterExt)
	counter, err := s.getCounterAndIncrease(ctx, errorCounterURL)
	if err != nil {
		response.CounterError = err.Error()
	}
	if counter > s.config.MaxRetries {
		response.RetryError = response.Error
		response.Status = base.StatusOK
		location := url.Path(request.SourceURL)
		retryDataURL := url.Join(s.config.JournalURL, base.RetryDataSubpath, request.EventID, location)
		if err := s.fs.Move(ctx, request.SourceURL, retryDataURL); err != nil {
			response.MoveError = err.Error()
		}
		return
	}

	if response.Retriable {
		response.RetryError = response.Error
		response.Error = ""
		return
	}

	if e := s.fs.Delete(ctx, request.SourceURL, option.NewObjectKind(true)); e != nil && response.NotFoundError == "" {
		response.NotFoundError = fmt.Sprintf("failed to delete: %v, %v", request.SourceURL, e)
	}
}

func (s *service) getCounterAndIncrease(ctx context.Context, URL string) (int, error) {
	ok, _ := s.fs.Exists(ctx, URL, option.NewObjectKind(true))
	counter := 0
	if ok {
		reader, err := s.fs.DownloadWithURL(ctx, URL)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to download counter :%v", URL)
		}
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to read counter :%v", URL)
		}
		counter = toolbox.AsInt(string(data))

	}
	counter++
	err := s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(fmt.Sprintf("%v", counter)))
	if err != nil {
		return counter, errors.Wrapf(err, "failed to update counter: %v", URL)
	}
	return counter, nil
}

func (s *service) Tail(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	response.TriggerURL = request.SourceURL

	defer s.OnDone(ctx, request, response)
	var err error

	if request.HasURLPrefix(s.config.LoadProcessPrefix) {
		err = s.runLoadProcess(ctx, request, response)
	} else if request.HasURLPrefix(s.config.BqJobPrefix) {
		err = s.runPostLoadActions(ctx, request, response)
	} else if request.HasURLPrefix(s.config.BatchPrefix) {
		err = s.runBatch(ctx, request, response)

	} else {
		err = s.tail(ctx, request, response)
	}

	if err != nil {
		response.SetIfError(err)
		if !response.Retriable {
			err = s.handlerProcessError(ctx, err, request, response)
		}
		if base.IsNotFoundError(err) {
			response.NotFoundError = err.Error()
			err = nil
		}
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
		response.Retriable = false
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
	return s.tryRecoverAndReport(ctx, rule, request, job.Actions, job.Job, response)
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
	info := s.newInfo(ctx, job, rule, job.Load)
	response.Info = info
	info.LoadURIs = job.Load.SourceUris
	load, err := s.buildLoadRequest(ctx, job, rule)
	if err != nil {
		return nil, err
	}

	info = s.newInfo(ctx, job, rule, load.JobConfigurationLoad)
	response.Info = info
	actions := rule.Actions().Expand(info)
	load.Actions = *actions
	if err = appendBatchAction(job.Window, &load.Actions); err != nil {
		return nil, err
	}
	activeURL := s.config.BuildActiveLoadURL(job.Info())
	doneURL := s.config.BuildDoneLoadURL(job.Info())
	s.appendLoadProcessFinalActions(activeURL, doneURL, load, info)
	if actions, err = s.addTransientDatasetActions(ctx, *info, job, rule, &load.Actions); err != nil {
		return nil, err
	}
	load.Actions = *actions
	if rule.Dest.HasSplit() {
		if _, err = s.updateTempTableScheme(ctx, load.JobConfigurationLoad, rule); err != nil {
			return nil, errors.Wrapf(err, "failed to update temp schema")
		}
	}
	if e := s.createLoadProcess(ctx, activeURL, load, info); e != nil {
		return nil, errors.Wrapf(err, "failed to create load job actions: %v", activeURL)
	}

	if base.IsLoggingEnabled() {
		toolbox.Dump(actions)
		loadMap := map[string]interface{}{}
		_ = toolbox.DefaultConverter.AssignConverted(&loadMap, load)
		toolbox.Dump(loadMap)
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

func (s *service) newInfo(ctx context.Context, job *Job, rule *config.Rule, load *bigquery.JobConfigurationLoad) *stage.Info {
	info := stage.New(job.GetSourceURI(), job.Dest(), job.EventID, "load", job.IDSuffix(), rule.Async, 0, rule.Info.URL)
	info.LoadURIs = job.Load.SourceUris
	info.TempTable = load.DestinationTable.DatasetId + "." + load.DestinationTable.TableId
	if rule.CounterURL != "" {
		counterURL := url.Join(rule.CounterURL, info.DestTable+base.CounterExt)
		counter, err := s.getCounterAndIncrease(ctx, counterURL)
		if err == nil {
			info.Counter = counter
		}
	}
	return info
}

//appendLoadProcessFinalActions append track action
func (s *service) appendLoadProcessFinalActions(activeURL, doneURL string, load *bq.LoadRequest, info *stage.Info) {
	moveRequest := storage.MoveRequest{SourceURL: activeURL, DestURL: doneURL, IsDestAbsoluteURL: true}
	moveAction, _ := task.NewAction(base.ActionMove, info, moveRequest)
	load.Actions.AddOnSuccess(moveAction)
}

func (s *service) createLoadProcess(ctx context.Context, URL string, loadJob *bq.LoadRequest, info *stage.Info) error {
	loadTrace, err := task.NewAction(base.ActionLoad, info, loadJob)
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
	if len(window.Locations) > 0 {
		URLsToDelete = append(URLsToDelete, window.Locations...)
	}
	deleteReq := storage.DeleteRequest{URLs: URLsToDelete}

	deleteAction, err := task.NewAction(base.ActionDelete, &actions.Info, deleteReq)
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
	var result = task.NewActions(parent, nil, nil)
	var onFailureAction *task.Actions
	if actions != nil {
		result.SourceURI = job.GetSourceURI()
		onFailureAction = actions.CloneOnFailure()
		result.AddOnFailure(actions.OnFailure...)
	}
	tableID := job.Load.DestinationTable.DatasetId + "." + job.Load.DestinationTable.TableId
	dropAction, err := task.NewAction(base.ActionDrop, &result.Info, bq.NewDropRequest(tableID, onFailureAction))
	if err != nil {
		return nil, err
	}
	actions.AddOnSuccess(dropAction)
	selectAll := sql.BuildSelect(job.Load.DestinationTable, job.Load.Schema, rule.Dest.TransientAlias, rule.Dest.UniqueColumns, rule.Dest.Transform, rule.Dest.SideInputs)
	selectAll = result.Info.ExpandText(selectAll)
	if rule.Dest.HasSplit() {
		return result, s.addSplitActions(ctx, selectAll, parent, job, rule, result, actions)
	}
	selectAll = strings.Replace(selectAll, "$WHERE", "", 1)
	destTable, _ := rule.Dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])
	partition := base.TablePartition(destTable.TableId)

	if len(rule.Dest.UniqueColumns) > 0 || partition != "" || len(rule.Dest.Transform) > 0 {
		query := bq.NewQueryRequest(selectAll, destTable, actions)
		query.Append = rule.IsAppend()
		queryAction, err := task.NewAction(base.ActionQuery, &result.Info, query)
		if err != nil {
			return nil, err
		}
		result.AddOnSuccess(queryAction)
	} else {
		source := base.EncodeTableReference(job.Load.DestinationTable)
		dest := base.EncodeTableReference(destTable)
		copyRequest := bq.NewCopyRequest(source, dest, actions)
		copyRequest.Append = rule.IsAppend()
		copyAction, err := task.NewAction(base.ActionCopy, &result.Info, copyRequest)
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

func (s *service) updateTempTableScheme(ctx context.Context, job *bigquery.JobConfigurationLoad, rule *config.Rule) (bool, error) {
	split := rule.Dest.Schema.Split
	if job.Schema == nil {
		return false, nil
	}
	extendedSchema := false
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
			extendedSchema = true
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
					return false, errors.Errorf("failed to lookup cluster column: %v", name)
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
	return extendedSchema, nil
}

func (s *service) addSchemaPatchAction(actions *task.Actions, dest *config.Destination, job *Job) (*task.Actions, error) {
	template := dest.Schema.Template
	if template == "" {
		template = job.DestTable
	}
	patch, err := task.NewAction(base.ActionQuery, &actions.Info, bq.NewPatchRequest(template, job.TempTable, actions))
	if err != nil {
		return nil, err
	}
	group := task.NewActions(actions.Info, nil, nil)
	group.AddOnSuccess(patch)
	return group, nil
}

func (s *service) addSplitActions(ctx context.Context, selectAll string, parent stage.Info, job *Job, rule *config.Rule, result, onDone *task.Actions) error {
	split := rule.Dest.Schema.Split
	next := onDone
	if next == nil {
		next = task.NewActions(parent, nil, nil)
	}
	for i := range split.Mapping {
		mapping := split.Mapping[i]
		destTable, _ := rule.Dest.CustomTableReference(mapping.Then, job.SourceCreated, job.Load.SourceUris[0])
		dest := strings.Replace(selectAll, "$WHERE", " WHERE  "+mapping.When+" ", 1)
		query := bq.NewQueryRequest(dest, destTable, next)
		query.Append = rule.IsAppend()
		queryAction, err := task.NewAction(base.ActionQuery, &onDone.Info, query)
		if err != nil {
			return err
		}
		group := task.NewActions(parent, nil, nil)
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
			queryAction, err := task.NewAction(base.ActionQuery, &onDone.Info, query)
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

//runLoadProcess this method allows rerun Activity/Done job as long original data files are present
func (s *service) runLoadProcess(ctx context.Context, request *contract.Request, response *contract.Response) error {
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

	for i, action := range actions {
		actions[i].Request = toolbox.ReplaceMapKeys(action.Request, replacement, true)
	}
	_, err = task.RunAll(ctx, s.Registry, actions)
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
		Actions:       rule.Actions(),
	}

	if job.Load, err = rule.Dest.NewJobConfigurationLoad(time.Now(), request.SourceURL); err != nil {
		return nil, err
	}
	return s.submitJob(ctx, job, rule, response)
}

func (s *service) updateSchemaIfNeeded(ctx context.Context, dest *config.Destination, tableReference *bigquery.TableReference, job *Job) error {
	var err error
	var table *bigquery.Table
	if dest.Schema.TransientTemplate != "" {
		templateReference, err := base.NewTableReference(dest.Schema.TransientTemplate)
		if err != nil {
			return errors.Wrapf(err, "failed to create table from TransientSchema: %v", dest.Schema.TransientTemplate)
		}
		if table, err = s.bq.Table(ctx, templateReference); err != nil {
			return errors.Wrapf(err, "failed to get tempalte table: %v", templateReference)
		}
		job.TempSchema = table.Schema
		updateLoadProcessSchema(table, job, dest)

	}

	if dest.Schema.Table != nil {
		job.DestSchema = dest.Schema.Table
		return nil
	}

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
		job.DestSchema = table.Schema
	}
	if dest.Schema.TransientTemplate == "" {
		updateLoadProcessSchema(table, job, dest)
	}
	return nil
}

func updateLoadProcessSchema(table *bigquery.Table, job *Job, dest *config.Destination) {
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
	return s.runInBatch(ctx, rule, batchWindow.Window, response)
}

func (s *service) runPostLoadActions(ctx context.Context, request *contract.Request, response *contract.Response) error {
	actions, err := task.NewActionFromURL(ctx, s.fs, request.SourceURL)
	if err != nil {
		object, _ := s.fs.Object(ctx, request.SourceURL, option.NewObjectKind(true))
		if object == nil {
			response.NotFoundError = err.Error()
			return nil
		}
		if actions, err = task.NewActionFromURL(ctx, s.fs, request.SourceURL); err != nil {
			return err
		}
	}
	response.Info = &actions.Info
	bqJob, err := s.bq.GetJob(ctx, s.config.ProjectID, actions.Job.JobReference.JobId)
	if err != nil {
		response.Retriable = base.IsRetryError(err)
		return errors.Wrapf(err, "failed to fetch job %v", actions.Job.JobReference.JobId)
	}

	job := base.Job(*bqJob)
	bqJobError := job.Error()
	if bqJobError != nil {
		rule := s.config.Get(ctx, actions.RuleURL, nil)
		if bqJobError = s.tryRecoverAndReport(ctx, rule, request, actions, bqJob, response); bqJobError == nil {
			return bqJobError
		}
	}

	if base.IsRetryError(bqJobError) {
		response.Retriable = true
		return bqJobError
	}

	if err := actions.Init(ctx, s.cfs); err != nil {
		return err
	}
	toRun := actions.ToRun(bqJobError, &job)
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
	rule := s.config.Get(ctx, window.RuleURL, window.Filter)
	if rule == nil {
		rule = s.config.Get(ctx, window.RuleURL, nil)
	}
	response.Rule = rule
	request.EventID = window.EventID
	job, err := s.runInBatch(ctx, rule, window, response)

	if err == nil || job == nil {
		if err != nil {
			response.Retriable = base.IsRetryError(err)
		}
		return err
	}
	return s.tryRecoverAndReport(ctx, rule, request, job.Actions, job.Job, response)
}

func (s *service) runInBatch(ctx context.Context, rule *config.Rule, window *batch.Window, response *contract.Response) (*Job, error) {
	response.Window = window
	response.BatchRunner = true
	if rule == nil {
		return nil, fmt.Errorf("rule was empyt for %v", window.RuleURL)
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
	return job, err
}

func (s *service) tryRecoverAndReport(ctx context.Context, rule *config.Rule, request *contract.Request, actions *task.Actions, job *bigquery.Job, response *contract.Response) error {
	err := base.JobError(job)
	if err == nil {
		return err
	}
	info := actions.Info
	if info.DestTable != "" {
		errorURL := url.Join(s.config.ErrorURL, info.DestTable, fmt.Sprintf("%v%v", request.EventID, base.ErrorExt))
		if e := s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(err.Error())); e != nil {
			response.UploadError = e.Error()
		}
	}
	return s.tryRecover(ctx, rule, request, actions, job, response)
}

func (s *service) tryRecover(ctx context.Context, rule *config.Rule, request *contract.Request, actions *task.Actions, job *bigquery.Job, response *contract.Response) error {
	configuration := actions.Job.Configuration
	response.Info = &actions.Info

	if configuration.Load == nil || len(configuration.Load.SourceUris) == 0 {
		err := base.JobError(job)
		response.Retriable = base.IsRetryError(err)
		return err
	}
	uris := status.NewURIs()
	response.URIs = *uris
	uris.Classify(ctx, s.fs, job)
	corruptedFileURL, invalidSchemaURL := s.getDataErrorsURLs(rule)
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
	load.Info = *load.Wrap(base.ActionReload)
	response.JobRef = bqJob.JobReference
	load.ProjectID = s.config.ProjectID
	loadJob, err := s.bq.Load(ctx, load)
	if err == nil {
		err = base.JobError(loadJob)
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
		if base.IsLoggingEnabled() {
			fmt.Printf("moving: %v %v, %v\n", sourceURL, destURL, err)
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

func (s *service) handlerProcessError(ctx context.Context, err error, request *contract.Request, response *contract.Response) error {
	info := response.Info
	if info == nil || err == nil {
		return err
	}
	activeURL := s.config.BuildActiveLoadURL(info)

	//Replay the whole load process - some individual BigQuery job can not be recovered
	if base.IsInternalError(err) || base.IsBackendError(err) {
		if exists, _ := s.fs.Exists(ctx, activeURL); exists {
			return s.replayLoadProcess(ctx, activeURL, request)
		}
	}
	response.SetIfError(err)
	if data, e := json.Marshal(response); e == nil {
		errorResponseURL := url.Join(s.config.ErrorURL, info.DestTable, fmt.Sprintf("%v%v", request.EventID, base.ResponseErrorExt))
		if e := s.fs.Upload(ctx, errorResponseURL, file.DefaultFileOsMode, bytes.NewReader(data)); e != nil {
			response.UploadError = e.Error()
		}

	}
	errorURL := url.Join(s.config.ErrorURL, info.DestTable, fmt.Sprintf("%v%v", request.EventID, base.ErrorExt))
	if e := s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(err.Error())); e != nil {
		response.UploadError = e.Error()
	}
	processErrorURL := url.Join(s.config.ErrorURL, info.DestTable, fmt.Sprintf("%v%v", request.EventID, base.ProcessExt))
	_ = s.fs.Copy(ctx, activeURL, processErrorURL)
	doneURL := s.config.BuildDoneLoadURL(info)
	_ = s.fs.Move(ctx, activeURL, doneURL)
	return err
}

func (s *service) replayLoadProcess(ctx context.Context, sourceURL string, request *contract.Request) error {
	bucket := url.Host(request.SourceURL)
	_, name := url.Split(sourceURL, gs.Scheme)
	loadJobURL := fmt.Sprintf("gs://%v/%v/%v", bucket, s.config.LoadProcessPrefix, name)
	return s.fs.Copy(ctx, sourceURL, loadJobURL)
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
