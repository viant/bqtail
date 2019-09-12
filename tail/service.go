package tail

import (
	"bqtail/base"
	"bqtail/service/bq"
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

type Service interface {
	//Tails appends data from source URL to matched BigQuery table
	Tail(ctx context.Context, request *contract.Request) *contract.Response
}

type service struct {
	task.Registry
	bq      bq.Service
	batch   batch.Service
	storage afs.Service
	config  *Config
}

func (s *service) Init(ctx context.Context) error {
	err := s.config.Init(ctx)
	if err != nil {
		return err
	}
	bqService, err := bigquery.NewService(ctx)
	if err != nil {
		return err
	}
	s.bq = bq.New(bqService, s.Registry, s.config.ProjectID, s.storage)
	s.batch = batch.New(s.config.BatchURL, s.storage)

	bq.InitRegistry(s.Registry, s.bq)
	storage.InitRegistry(s.Registry, storage.New(s.storage))
	return err
}

func (s *service) Tail(ctx context.Context, request *contract.Request) *contract.Response {
	response := contract.NewResponse(request.EventID)
	defer response.SetTimeTaken(response.Started)
	err := s.tail(ctx, request, response)
	if err != nil {
		response.SetIfError(err)
	}
	return response
}

func (s *service) tail(ctx context.Context, request *contract.Request, response *contract.Response) error {
	route := s.config.Routes.Match(request.SourceURL)
	if route == nil {
		return nil
	}
	response.Matched = true
	response.MatchedURL = request.SourceURL
	source, err := s.storage.Object(ctx, request.SourceURL)
	if err != nil {
		return errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	if route.Batch != nil {
		return s.tailInBatch(ctx, source, route, request, response)
	}
	return s.tailIndividually(ctx, source, route, request, response)
}

func (s *service) onDone(ctx context.Context, job *Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	baseURL := s.config.OutputURL(job.Status == base.StatusError)
	name := path.Join(job.SourceCreated.Format(dateLayout), path.Join(base.DecodePathSeparator(job.Dest()), job.EventID, base.TailJob+base.JobElement+base.JobExt))
	URL := url.Join(baseURL, name)
	return s.storage.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

func (s *service) buildLoadRequest(ctx context.Context, job *Job, dest *config.Destination) (*bq.LoadRequest, error) {
	tableReference, err := dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])
	if err != nil {
		return nil, err
	}
	if err = s.updateSchemaIfNeeded(ctx, dest, tableReference); err != nil {
		return nil, err
	}
	result := &bq.LoadRequest{Append: true}
	job.Load.DestinationTable = tableReference
	result.JobID = path.Join(job.Dest(), job.EventID, base.DispatchJob)

	if dest.TransientDataset != "" {
		tableReference.DatasetId = dest.TransientDataset
		tableReference.TableId += "_" + job.EventID
	}
	job.Load.Schema = dest.Schema.Table
	if job.Load.Schema == nil && dest.Schema.Autodetect {
		job.Load.Autodetect = dest.Schema.Autodetect
	}
	result.JobConfigurationLoad = job.Load
	return result, nil

}

func (s *service) submitJob(ctx context.Context, job *Job, route *config.Route, response *contract.Response) (err error) {
	if len(job.Load.SourceUris) == 0 {
		return fmt.Errorf("SourceUris was empty")
	}
	var load *bq.LoadRequest
	if load, err = s.buildLoadRequest(ctx, job, route.Dest); err != nil {
		return err
	}

	actions := route.Actions.Expand(&base.Expandable{SourceURLs: job.Load.SourceUris})
	actions.JobID = path.Join(job.Dest(), job.EventID, base.DispatchJob)
	if route.Dest.TransientDataset != "" {
		if actions, err = s.addTransientDatasetActions(ctx, load.JobID, job, route, actions); err != nil {
			return err
		}
	}
	load.Actions = *actions
	defer func() {
		response.SetIfError(err)
		job.SetIfError(err)
		if e := s.onDone(ctx, job); e != nil && err == nil {
			err = e
		}
	}()

	job.Job, err = s.bq.Load(ctx, load)
	if err == nil {
		err = bq.JobError(job.Job)
		response.JobRef = job.Job.JobReference
	}
	return err
}

func (s *service) addTransientDatasetActions(ctx context.Context, parentJobID string, job *Job, route *config.Route, actions *task.Actions) (*task.Actions, error) {
	var result = task.NewActions(actions.Async, actions.DeferTaskURL, parentJobID, nil, nil)
	dropDDL := fmt.Sprintf("DROP TABLE %v.%v", job.Load.DestinationTable.DatasetId, job.Load.DestinationTable.TableId)
	dropAction, err := task.NewAction("query", bq.NewQueryRequest(dropDDL, nil, nil))
	if err != nil {
		return nil, err
	}
	actions.AddOnSuccess(dropAction)
	destTable, _ := route.Dest.TableReference(job.SourceCreated, job.Load.SourceUris[0])
	selectAll := sql.BuildSelect(job.Load.DestinationTable, job.Load.Schema, route.Dest.UniqueColumns)
	query := bq.NewQueryRequest(selectAll, destTable, actions)
	query.Append = true
	queryAction, err := task.NewAction("query", query)
	if err != nil {
		return nil, err
	}
	result.AddOnSuccess(queryAction)
	return result, nil
}

func (s *service) tailIndividually(ctx context.Context, source store.Object, route *config.Route, request *contract.Request, response *contract.Response) error {
	object, err := s.storage.Object(ctx, request.SourceURL)
	if err != nil {
		return errors.Wrapf(err, "event source not found:%v", request.SourceURL)
	}
	job := &Job{
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: object.ModTime(),
		Load:          &bigquery.JobConfigurationLoad{},
	}
	job.Load.SourceUris = []string{request.SourceURL}
	return s.submitJob(ctx, job, route, response)
}

func (s *service) updateSchemaIfNeeded(ctx context.Context, dest *config.Destination, tableReference *bigquery.TableReference) error {
	if dest.Schema.Table != nil {
		return nil
	}
	if dest.Schema.Template != nil {
		table, err := s.bq.Table(ctx, dest.Schema.Template)
		if err != nil {
			return errors.Wrapf(err, "failed to updated schema for: %v", dest.Schema.Template)
		}
		dest.Schema.Table = table.Schema
		return nil
	}

	if dest.TransientDataset != "" {
		table, err := s.bq.Table(ctx, tableReference)
		if err != nil {
			return errors.Wrapf(err, "failed to get schema for %v.%v", tableReference.DatasetId, tableReference.TableId)
		}
		dest.Schema.Table = table.Schema
	}
	return nil
}

func (s *service) tailInBatch(ctx context.Context, source store.Object, route *config.Route, request *contract.Request, response *contract.Response) error {
	err := s.batch.Add(ctx, source.ModTime(), request, route)
	if err != nil {
		return err
	}
	response.Batched = true
	window, err := s.batch.TryAcquireWindow(ctx, request, route)
	if window == nil {
		return err
	}
	response.BatchRunner = true
	if err = s.batch.MatchWindowData(ctx, time.Now(), window, route); err != nil {
		return err
	}
	job := &Job{
		Status:        base.StatusOK,
		EventID:       request.EventID,
		SourceCreated: source.ModTime(),
		Window:        window,
		Load:          &bigquery.JobConfigurationLoad{},
	}
	job.Load.SourceUris = make([]string, 0)
	for i := range window.Datafiles {
		job.Load.SourceUris = append(job.Load.SourceUris, window.Datafiles[i].SourceURL)
	}
	return s.submitJob(ctx, job, route, response)
}

//New creates a new service
func New(ctx context.Context, config *Config) (Service, error) {
	srv := &service{
		config:   config,
		storage:  afs.New(),
		Registry: task.NewRegistry(),
	}
	return srv, srv.Init(ctx)
}
