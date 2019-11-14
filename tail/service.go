package tail

import (
	"bqtail/base"
	"bqtail/service/bq"
	"bqtail/service/secret"
	"bqtail/service/slack"
	"bqtail/service/storage"
	"bqtail/tail/batch"
	"bqtail/tail/config"
	"bqtail/tail/contract"
	"bqtail/tail/sql"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	store "github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"path"
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
	config *Config
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
	s.batch = batch.New(s.config.BatchURL, s.fs)

	bq.InitRegistry(s.Registry, s.bq)
	storage.InitRegistry(s.Registry, storage.New(s.fs))
	return err
}

func (s *service) Tail(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	response.TriggerURL = request.SourceURL
	defer response.SetTimeTaken(response.Started)
	err := s.tail(ctx, request, response)
	if err != nil {
		response.SetIfError(err)
	}
	return response
}

func (s *service) tail(ctx context.Context, request *contract.Request, response *contract.Response) error {
	if err := s.config.ReloadIfNeeded(ctx, s.fs); err != nil {
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
	if exists, err := s.fs.Exists(ctx, request.SourceURL); !exists {
		if err != nil {
			response.NotFoundError = err.Error()
		}
		response.Status = base.StatusNotFound
		return nil
	}
	response.Rule = rule
	response.Matched = true
	response.MatchedURL = request.SourceURL
	source, err := s.fs.Object(ctx, request.SourceURL)
	if err != nil {
		return errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	if rule.Batch != nil {
		return s.tailInBatch(ctx, source, rule, request, response)
	}
	return s.tailIndividually(ctx, source, rule, request, response)
}

func (s *service) onDone(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	baseURL := s.config.OutputURL(job.Status == base.StatusError)
	name := path.Join(job.SourceCreated.Format(dateLayout), path.Join(base.DecodePathSeparator(job.Dest()), job.EventID, base.TailJob+base.JobElement+base.JobExt))
	URL := url.Join(baseURL, name)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *service) buildLoadRequest(ctx context.Context, job *Job, rule *config.Rule) (*bq.LoadRequest, error) {
	dest := rule.Dest
	tableReference, err := dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])
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
	result.JobID = getJobID(job)
	return result, nil

}

func getJobID(job *Job) string {
	return path.Join(job.Dest(), job.EventID, job.IDSuffix())
}

func (s *service) submitJob(ctx context.Context, job *Job, rule *config.Rule, response *contract.Response) (err error) {
	if len(job.Load.SourceUris) == 0 {
		return fmt.Errorf("sourceUris was empty")
	}
	var load *bq.LoadRequest
	if load, err = s.buildLoadRequest(ctx, job, rule); err != nil {
		return err
	}
	actions := rule.Actions.Expand(&base.Expandable{SourceURLs: job.Load.SourceUris})
	actions.JobID = path.Join(job.Dest(), job.EventID, job.IDSuffix())
	if err = appendBatchAction(job.Window, actions); err == nil {
		actions, err = s.addTransientDatasetActions(ctx, load.JobID, job, rule, actions)
	}
	if err != nil {
		return err
	}
	if rule.Dest.HasSplit() {
		if err = s.updateTempTableScheme(ctx, load.JobConfigurationLoad, rule); err != nil {
			return errors.Wrapf(err, "failed to upload load schema")
		}
	}
	load.Actions = *actions
	defer func() {
		job.Actions = actions
		response.SetIfError(err)
		job.SetIfError(err)
		if e := s.onDone(ctx, job); e != nil && err == nil {
			err = e
		}
	}()

	var bqJob *bigquery.Job
	bqJob, err = s.bq.Load(ctx, load)
	if err == nil {
		job.JobStatus = bqJob.Status
		job.Statistics = bqJob.Statistics
		err = base.JobError(bqJob)
		response.JobRef = bqJob.JobReference
	}
	return err
}

func appendBatchAction(window *batch.Window, actions *task.Actions) error {
	if window == nil {
		return nil
	}
	URLsToDelete := make([]string, 0)
	URLsToDelete = append(URLsToDelete, window.URL)
	for _, datafile := range window.Datafiles {
		URLsToDelete = append(URLsToDelete, datafile.URL)
	}
	deleteReq := storage.DeleteRequest{URLs: URLsToDelete}
	deleteAction, err := task.NewAction("delete", deleteReq)
	if err != nil {
		return err
	}
	actions.AddOnSuccess(deleteAction)
	return nil
}

func (s *service) addTransientDatasetActions(ctx context.Context, parentJobID string, job *Job, rule *config.Rule, actions *task.Actions) (*task.Actions, error) {
	if rule.Dest.TransientDataset == "" {
		return actions, nil
	}
	var result = task.NewActions(actions.Async, actions.DeferTaskURL, parentJobID, nil, nil)
	var onFailureAction *task.Actions
	if actions != nil {
		result.SourceURL = actions.SourceURL
		onFailureAction = actions.CloneOnFailure()
		result.AddOnFailure(actions.OnFailure...)
	}

	tableID := base.TableID(job.Load.DestinationTable.TableId)
	dropDDL := fmt.Sprintf("DROP TABLE %v.%v", job.Load.DestinationTable.DatasetId, tableID)
	dropAction, err := task.NewAction("query", bq.NewQueryRequest(dropDDL, nil, onFailureAction))
	if err != nil {
		return nil, err
	}
	actions.AddOnSuccess(dropAction)
	selectAll := sql.BuildSelect(job.Load.DestinationTable, job.Load.Schema, rule.Dest.UniqueColumns, rule.Dest.Transform)
	if rule.Dest.HasSplit() {
		return result, s.addSplitActions(ctx, selectAll, parentJobID, job, rule, result, actions)
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
				column := getColumn(job.Schema.Fields, split.ClusterColumns[i]);
				if column == nil {
					return errors.Errorf("failed to lookup cluster column: ", name)
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

func (s *service) addSplitActions(ctx context.Context, selectAll string, parentJobID string, job *Job, rule *config.Rule, result, onDone *task.Actions) error {
	split := rule.Dest.Schema.Split

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

			query := bq.NewQueryRequest(DML, nil, nil)
			query.Append = rule.IsAppend()
			queryAction, err := task.NewAction("query", query)
			if err != nil {
				return err
			}
			result.AddOnSuccess(queryAction)
		}
	}

	for i := range split.Mapping {
		next := task.NewActions(rule.Async, result.DeferTaskURL, result.JobID, nil, nil)
		if i == len(split.Mapping)-1 {
			next = onDone
		}
		mapping := split.Mapping[i]
		destTable, _ := rule.Dest.CustomTableReference(mapping.Then, job.SourceCreated, job.Load.SourceUris[0])
		dest := strings.Replace(selectAll, "$WHERE", " WHERE  "+mapping.When+" ", 1)
		query := bq.NewQueryRequest(dest, destTable, next)
		query.Append = rule.IsAppend()
		queryAction, err := task.NewAction("query", query)
		if err != nil {
			return err
		}
		result.AddOnSuccess(queryAction)
	}
	return nil
}

func (s *service) tailIndividually(ctx context.Context, source store.Object, rule *config.Rule, request *contract.Request, response *contract.Response) error {
	object, err := s.fs.Object(ctx, request.SourceURL)
	if err != nil {
		return errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	job := &Job{
		Rule:          rule,
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: object.ModTime(),
	}
	if job.Load, err = rule.Dest.NewJobConfigurationLoad(time.Now(), request.SourceURL); err != nil {
		return err
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

func (s *service) tailInBatch(ctx context.Context, source store.Object, rule *config.Rule, request *contract.Request, response *contract.Response) error {
	scheduled, err := s.batch.Add(ctx, source.ModTime(), request, rule)
	if err != nil {
		return errors.Wrapf(err, "failed to add event trace file")
	}
	if scheduled == nil {
		response.Status = base.StatusDuplicate
		return nil
	}
	response.Batched = true
	response.ScheduledURL = scheduled.URL()
	request.ScheduleURL = scheduled.URL()
	batchWindow, err := s.batch.TryAcquireWindow(ctx, request, rule)
	if batchWindow == nil || err != nil {
		if err != nil {
			return errors.Wrapf(err, "failed to acquire batch window")
		}
	}

	response.BatchingEventID = batchWindow.BatchingEventID
	if batchWindow.Window == nil {
		return nil
	}
	window := batchWindow.Window
	response.Window = window
	response.BatchRunner = true
	if err = s.batch.MatchWindowData(ctx, time.Now(), window, rule); err != nil {
		return errors.Wrapf(err, "failed to match window data")
	}
	if window.LostOwnership {
		return nil
	}
	job := &Job{
		Rule:          rule,
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: source.ModTime(),
		Window:        window,
	}
	var URIs = make([]string, 0)
	var unique = map[string]bool{}
	for i := range window.Datafiles {
		if unique[window.Datafiles[i].SourceURL] {
			continue
		}
		unique[window.Datafiles[i].SourceURL] = true
		URIs = append(URIs, window.Datafiles[i].SourceURL)
	}
	if job.Load, err = rule.Dest.NewJobConfigurationLoad(time.Now(), URIs...); err != nil {
		return err
	}

	return s.submitJob(ctx, job, rule, response)
}

//New creates a new service
func New(ctx context.Context, config *Config) (Service, error) {
	srv := &service{
		config:   config,
		fs:       afs.New(),
		Registry: task.NewRegistry(),
	}
	return srv, srv.Init(ctx)
}
