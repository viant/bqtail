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
	request.Attempt++
	err := s.tail(ctx, request, response)
	if err != nil {
		response.SetIfError(err)
	}
	if base.IsBackendError(response.Error) {
		if request.Attempt < s.config.MaxRetries {
			response.Error = ""
			response.Status = base.StatusOK
			if err = s.tail(ctx, request, response); err != nil {
				response.SetIfError(err)
			}
		}
	}
	return response
}

func (s *service) tail(ctx context.Context, request *contract.Request, response *contract.Response) error {
	if err := s.config.ReloadIfNeeded(ctx, s.fs); err != nil {
		return err
	}
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
	if exists, _ := s.fs.Exists(ctx, request.SourceURL); !exists {
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

func (s *service) submitJob(ctx context.Context, job *Job, route *config.Rule, response *contract.Response) (err error) {
	if len(job.Load.SourceUris) == 0 {
		return fmt.Errorf("sourceUris was empty")
	}
	var load *bq.LoadRequest
	if load, err = s.buildLoadRequest(ctx, job, route); err != nil {
		return err
	}
	actions := route.Actions.Expand(&base.Expandable{SourceURLs: job.Load.SourceUris})
	actions.JobID = path.Join(job.Dest(), job.EventID, job.IDSuffix())

	if err = appendBatchAction(job.Window, actions); err == nil {
		actions, err = s.addTransientDatasetActions(ctx, load.JobID, job, route, actions)
	}
	if err != nil {
		return err
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

func (s *service) addTransientDatasetActions(ctx context.Context, parentJobID string, job *Job, route *config.Rule, actions *task.Actions) (*task.Actions, error) {
	if route.Dest.TransientDataset == "" {
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
	destTable, _ := route.Dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])
	selectAll := sql.BuildSelect(job.Load.DestinationTable, job.Load.Schema, route.Dest.UniqueColumns)
	partition := base.TablePartition(destTable.TableId)
	if len(route.Dest.UniqueColumns) > 0 || partition != "" {
		query := bq.NewQueryRequest(selectAll, destTable, actions)
		query.Append = route.IsAppend()
		queryAction, err := task.NewAction("query", query)
		if err != nil {
			return nil, err
		}
		result.AddOnSuccess(queryAction)
	} else {
		source := base.EncodeTableReference(job.Load.DestinationTable)
		dest := base.EncodeTableReference(destTable)
		copyRequest := bq.NewCopyRequest(source, dest, actions)
		copyRequest.Append = route.IsAppend()
		copyAction, err := task.NewAction("copy", copyRequest)
		if err != nil {
			return nil, err
		}
		result.AddOnSuccess(copyAction)
	}
	return result, nil
}

func (s *service) tailIndividually(ctx context.Context, source store.Object, route *config.Rule, request *contract.Request, response *contract.Response) error {
	object, err := s.fs.Object(ctx, request.SourceURL)
	if err != nil {
		return errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	job := &Job{
		Rule:          route,
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: object.ModTime(),
	}
	if job.Load, err = route.Dest.NewJobConfigurationLoad(time.Now(), request.SourceURL); err != nil {
		return err
	}
	return s.submitJob(ctx, job, route, response)
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
			return err
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

func (s *service) tailInBatch(ctx context.Context, source store.Object, route *config.Rule, request *contract.Request, response *contract.Response) error {
	err := s.batch.Add(ctx, source.ModTime(), request, route)
	if err != nil {
		return err
	}
	response.Batched = true
	window, err := s.batch.TryAcquireWindow(ctx, request, route)
	if window == nil {
		return err
	}
	response.Window = window
	response.BatchRunner = true
	if err = s.batch.MatchWindowData(ctx, time.Now(), window, route); err != nil {
		return err
	}
	job := &Job{
		Rule:          route,
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: source.ModTime(),
		Window:        window,
	}
	var URIs = make([]string, 0)
	for i := range window.Datafiles {
		URIs = append(URIs, window.Datafiles[i].SourceURL)
	}
	if job.Load, err = route.Dest.NewJobConfigurationLoad(time.Now(), URIs...); err != nil {
		return err
	}

	return s.submitJob(ctx, job, route, response)
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
