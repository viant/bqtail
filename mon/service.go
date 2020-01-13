package mon

import (
	"bqtail/base"
	"bqtail/mon/info"
	"bqtail/service/bq"
	"bqtail/sortable"
	"bqtail/stage"
	"bqtail/tail"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/toolbox"
	"io/ioutil"
	"sort"
	"strings"

	//add gcs storage API
	_ "github.com/viant/afsc/gs"
	"path"
	"sync"
	"time"
)

const maxErrors = 40

//Service represents monitoring service
type Service interface {
	//Check checks un process file and mirror errors
	Check(context.Context, *Request) *Response
}

type service struct {
	fs afs.Service
	*tail.Config
}

//Check checks triggerBucket and error
func (s *service) Check(ctx context.Context, request *Request) *Response {
	response := NewResponse()
	err := s.check(ctx, request, response)
	if err != nil {
		response.Error = err.Error()
		response.Status = base.StatusError
	}
	return response
}

func (s *service) check(ctx context.Context, request *Request, response *Response) (err error) {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(5)
	infoDest := map[string]*Info{}
	_ = s.Config.ReloadIfNeeded(ctx, s.fs)
	var active, doneLoads activeLoads
	var schedules batches
	var errors []*info.Error
	var stages []*stage.Info
	go func() {
		defer waitGroup.Done()
		var e error
		if active, e = s.getActiveLoads(ctx); e != nil {
			err = e
		}
	}()

	go func() {
		defer waitGroup.Done()
		var e error
		if errors, e = s.getErrors(ctx, request.Recency); e != nil {
			err = e
		}
	}()

	go func() {
		defer waitGroup.Done()
		var e error
		if !request.IncludeDone {
			return
		}
		if doneLoads, e = s.getRecentlyDoneLoads(ctx); e != nil {
			err = e
		}
	}()
	go func() {
		defer waitGroup.Done()
		var e error
		if schedules, e = s.getScheduledBatches(ctx); e != nil {
			err = e
		}
	}()
	go func() {
		defer waitGroup.Done()
		var e error
		if stages, e = s.getLoadStages(ctx); e != nil {
			err = e
		}
	}()
	waitGroup.Wait()
	if len(active) > 0 {
		s.updateActiveLoads(active, infoDest)
	}
	if len(doneLoads) > 0 {
		s.updateRecentlyDone(doneLoads, infoDest)
	}
	if len(schedules) > 0 {
		s.updateBatches(schedules, infoDest)
	}
	if len(stages) > 0 {
		s.updateStages(stages, infoDest)
	}

	if len(errors) > 0 {
		s.updateErrors(errors, infoDest)
	}

	var keys = make([]string, 0)
	for k, inf := range infoDest {
		permissionError := false
		response.Info.Add(inf)

		if inf.Error != nil && len(inf.Error.DataURLs) > 0 {
			if response.Status == base.StatusOK {
				if permissionError = inf.Error.IsPermission; !permissionError {
					response.Status = base.StatusError
				}
			}
			if inf.Error.IsPermission {
				response.PermissionError = inf.Error.Message
			}
			if inf.Error.IsSchema {
				response.SchemaError = inf.Error.Message
			}
			if inf.Error.IsCorrupted {
				response.CorruptedError = inf.Error.Message
			}

		}
		rule := inf.rule
		if rule == nil {
			rule = s.Config.Get(ctx, inf.RuleURL, nil)
		}
		if rule != nil {
			inf.Corrupted, _ = s.getURLMetrics(ctx, rule.CorruptedFileURL, inf, request.Recency)
			inf.InvalidSchema, _ = s.getURLMetrics(ctx, rule.InvalidSchemaURL, inf, request.Recency)
		}

		if inf.Activity != nil {
			if inf.Activity.Running != nil {
				stalledDuration := 90 * time.Minute
				if rule != nil && rule.StalledThresholdInSec > 0 {
					stalledDuration = time.Duration(rule.StalledThresholdInSec) * time.Second
				}

				if time.Now().Sub(*inf.Activity.Running.Min) > stalledDuration {
					metric := response.Stalled.GetOrCreate(inf.Destination.Table)
					if response.Status == base.StatusOK && !permissionError {
						response.Status = base.StatusStalled
					}
					metric.AddEvent(*inf.Activity.Running.Min)
				}
			}

		}
		keys = append(keys, k)

	}
	sort.Strings(keys)
	for _, k := range keys {
		response.Dest = append(response.Dest, infoDest[k])
	}

	if request.DestPath != "" {
		data, err := json.Marshal(response)
		if err != nil {
			response.UploadError = err.Error()
			return nil
		}

		baseURL := fmt.Sprintf("gs://%v/%v", request.DestBucket, strings.Trim(request.DestPath, "/"))
		destURL := url.Join(baseURL, fmt.Sprintf("%v.json", time.Now().UnixNano()))
		if err = s.fs.Upload(ctx, destURL, file.DefaultFileOsMode, bytes.NewReader(data)); err != nil {
			response.UploadError = err.Error()
		}
	}
	return nil
}

func (s *service) getURLMetrics(ctx context.Context, URL string, inf *Info, recencyExpr string) (*info.Metric, error) {
	if URL == "" {
		return nil, nil
	}
	inThePast, err := toolbox.TimeAt(recencyExpr + "ago")
	if err != nil {
		return nil, err
	}
	recency := time.Now().Sub(*inThePast)
	objects, err := s.fs.List(ctx, URL, option.NewRecursive(true))
	if err != nil || len(objects) == 0 {
		return nil, err
	}
	var result = info.NewMetric()
	for _, candidate := range objects {
		if candidate.IsDir() {
			continue
		}
		if time.Now().Sub(candidate.ModTime()) > recency {
			continue
		}
		result.AddEvent(candidate.ModTime())
	}
	return result, nil
}

func (s *service) updateActiveLoads(loadsInfo activeLoads, infoDest map[string]*Info) {
	for k, v := range loadsInfo.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Running == nil {
			inf.Activity.Running = info.NewMetric()
		}
		inf.Activity.Running.Add(&v.Metric, true)
	}
}

func (s *service) updateRecentlyDone(recentLoads activeLoads, infoDest map[string]*Info) {
	for k, v := range recentLoads.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Done == nil {
			inf.Activity.Done = info.NewMetric()
		}
		inf.Activity.Done.Add(&v.Metric, false)
	}
}

func (s *service) updateBatches(batchSlice batches, infoDest map[string]*Info) {
	for k, v := range batchSlice.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Scheduled == nil {
			inf.Activity.Scheduled = info.NewMetric()
		}
		inf.Activity.Scheduled.Add(&v.Metric, true)
	}
}

func (s *service) updateStages(stages []*stage.Info, infoDest map[string]*Info) {
	for _, stageInfo := range stages {
		inf := s.getInfo(stageInfo.DestTable, infoDest)
		stageKey := fmt.Sprintf("%04d-%v", stageInfo.Sequence(), stageInfo.Action)
		stageValue := inf.Activity.Stages.GetOrCreate(stageKey)
		stageValue.AddEvent(stageInfo.SourceTime)
	}

}

func (s *service) getErrors(ctx context.Context, recencyExpr string) ([]*info.Error, error) {
	destFolders, err := s.fs.List(ctx, s.Config.ErrorURL)
	if err != nil {
		return nil, err
	}

	var result = make([]*info.Error, 0)
	for _, folder := range destFolders {
		if url.Equals(folder.URL(), s.Config.ErrorURL) || !folder.IsDir() {
			continue
		}
		dest := folder.Name()
		modifiedAfter := getErrorLoopback(recencyExpr)
		files, err := s.fs.List(ctx, folder.URL(), option.NewPage(0, maxErrors), matcher.NewModification(nil, &modifiedAfter))
		if err != nil {
			return nil, err
		}
		infoError, err := s.getError(ctx, dest, files)
		if err != nil || infoError == nil {
			continue
		}
		result = append(result, infoError)
	}
	return result, nil
}

func getErrorLoopback(recencyExpr string) time.Time {
	modifiedAfter := time.Now().Add(-time.Hour)
	if recencyExpr != "" {
		if ! strings.Contains(strings.ToLower(recencyExpr), "ago") {
			recencyExpr += "Ago"
		}
		if inThePast, err := toolbox.TimeAt(recencyExpr); err == nil {
			modifiedAfter = *inThePast
		}
	}
	return modifiedAfter
}

func (s *service) getInfo(destKey string, infoDest map[string]*Info) *Info {
	rule := s.Config.MatchByTable(destKey)
	if rule != nil {
		destKey = rule.Dest.Table
	}
	inf, ok := infoDest[destKey]
	if !ok {
		inf = NewInfo()
		if rule != nil {
			inf.Destination.Table = rule.Dest.Table
			inf.Destination.RuleURL = rule.Info.URL
		}
		if destKey != "" {
			infoDest[destKey] = inf
		}
	}
	inf.rule = rule
	return inf
}

func (s *service) getError(ctx context.Context, dest string, files []storage.Object) (*info.Error, error) {
	if len(files) == 0 {
		return nil, nil
	}
	sortedFiles := sortable.NewObjects(files[1:], sortable.ByModTime)
	result := newError(dest, sortedFiles)
	if result == nil {
		return nil, nil
	}

	errName := fmt.Sprintf("%v%v", result.EventID, base.ErrorExt)
	processName := fmt.Sprintf("%v%v", result.EventID, base.ProcessExt)
	errorURL := url.Join(files[0].URL(), errName)

	reader, err := s.fs.DownloadWithURL(ctx, errorURL)
	if err == nil {
		data, err := ioutil.ReadAll(reader)
		_ = reader.Close()
		if err == nil {
			result.Message = string(data)
		}
	}

	processURL := url.Join(files[0].URL(), processName)
	result.ProcessURL = processURL

	if processURL != "" {
		actions := []*task.Action{}
		if reader, err := s.fs.DownloadWithURL(ctx, processURL); err == nil {
			if err = json.NewDecoder(reader).Decode(&actions); err == nil && len(actions) > 0 {
				result.DataURLs, _ = s.getUnprocessedFiles(ctx, actions[0])
			}
			_ = reader.Close()
		}
	}

	result.IsPermission = base.IsPermissionDenied(fmt.Errorf(result.Message))
	if result.IsSchema = IsSchemaError(result.Message); !result.IsSchema {
		result.IsCorrupted = IsCorruptedError(result.Message)
	}
	return result, nil
}

//newError create a en error object
func newError(dest string, sortedFiles *sortable.Objects) *info.Error {
	eventID := ""
	errorEventID := ""
	count := len(sortedFiles.Elements)
	if count > maxErrors {
		count = maxErrors
	}
	result := &info.Error{Destination: dest}
	for i := 0; i < count; i++ {
		object := sortedFiles.Elements[i]
		ext := path.Ext(object.Name())

		if ext == base.ProcessExt {
			eventID = strings.Replace(object.Name(), ext, "", 1)
			result.ModTime = object.ModTime()
			break
		}
		if ext == base.ErrorExt {
			errorEventID = strings.Replace(object.Name(), ext, "", 1)
			result.ModTime = object.ModTime()
		}
	}
	if eventID == "" {
		if eventID = errorEventID; eventID == "" {
			return nil
		}
	}
	result.EventID = eventID
	return result
}

func (s *service) getLoadStages(ctx context.Context) ([]*stage.Info, error) {
	result := make([]*stage.Info, 0)
	return result, s.listLoadStages(ctx, &result)
}

func (s *service) listLoadStages(ctx context.Context, result *[]*stage.Info) error {
	objects, err := s.fs.List(ctx, s.AsyncTaskURL)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) == base.WindowExt {
			continue
		}

		stageInfo := stage.Parse(object.Name())
		stageInfo.SourceTime = object.ModTime()
		*result = append(*result, stageInfo)
	}
	return nil
}

func (s *service) getScheduledBatches(ctx context.Context) (batches, error) {
	var result = make([]*batch, 0)
	objects, err := s.fs.List(ctx, s.AsyncTaskURL)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) != base.WindowExt {
			continue
		}
		result = append(result, parseBatch(object.Name()))
	}
	return result, nil
}

func (s *service) getActiveLoads(ctx context.Context) (activeLoads, error) {
	result := activeLoads{}
	err := s.listLoadProcess(ctx, s.Config.ActiveLoadProcessURL, s.Config.ActiveLoadProcessURL, &result)
	return result, err
}

func (s *service) getRecentlyDoneLoads(ctx context.Context) (activeLoads, error) {
	result := activeLoads{}
	err := s.listDoneLoads(ctx, s.Config.DoneLoadProcessURL, &result)
	return result, err
}

func (s *service) listLoadProcess(ctx context.Context, baseURL, URL string, result *activeLoads) error {
	objects, err := s.fs.List(ctx, URL)
	if err != nil {
		return err
	}
	for i := range objects {
		object := objects[i]
		if url.Equals(object.URL(), URL) {
			continue
		}
		if object.IsDir() {
			if err = s.listLoadProcess(ctx, baseURL, object.URL(), result); err != nil {
				return err
			}
			continue
		}
		*result = append(*result, parseLoad(baseURL, object.URL(), object.ModTime()))

	}
	return nil
}

func (s *service) listDoneLoads(ctx context.Context, baseURL string, result *activeLoads) error {
	objects, err := s.fs.List(ctx, baseURL)
	if err != nil || len(objects) <= 1 {
		return err
	}
	var destLocations = make(map[string]storage.Object)
	sortedTables := sortable.NewObjects(objects[1:], sortable.ByModTime)

	for i, destObject := range sortedTables.Elements {
		key := destObject.Name()
		if rule := s.Config.MatchByTable(key); rule != nil {
			key = rule.Dest.Table
		}
		destLocations[key] = sortedTables.Elements[i]
	}

	for _, destObject := range destLocations {
		if url.Equals(destObject.URL(), baseURL) {
			continue
		}
		hourDone, err := s.fs.List(ctx, destObject.URL())
		if err != nil {
			return err
		}
		if len(hourDone) == 0 {
			continue
		}
		sortedHours := sortable.NewObjects(hourDone[1:], sortable.ByModTime)
		if len(sortedHours.Elements) == 0 {
			continue
		}
		recentHour := sortedHours.Elements[len(sortedHours.Elements)-1]
		if err = s.listLoadProcess(ctx, baseURL, recentHour.URL(), result); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) updateErrors(errors []*info.Error, infos map[string]*Info) {
	for i := range errors {
		inf := s.getInfo(errors[i].Destination, infos)
		inf.Error = errors[i]
	}
}

//getUnprocessedFiles return get unprocessed files
func (s *service) getUnprocessedFiles(ctx context.Context, action *task.Action) ([]string, error) {
	loadRequest := &bq.LoadRequest{}
	err := toolbox.DefaultConverter.AssignConverted(&loadRequest, action.Request)
	if err != nil {
		return nil, err
	}
	URICount := len(loadRequest.SourceUris)
	if URICount == 0 {
		return nil, err
	}
	URIIndexes := []int{0}
	if URICount > 1 {
		URIIndexes = append(URIIndexes, URICount-1)
	}
	for i := range URIIndexes {
		if exists, _ := s.fs.Exists(ctx, loadRequest.SourceUris[i], option.NewObjectKind(true)); exists {
			return loadRequest.SourceUris, nil
		}
	}
	return nil, err
}

//New creates monitoring service
func New(ctx context.Context, config *tail.Config) (Service, error) {
	fs := afs.New()
	cfs := cache.New(config.URL, fs)
	err := config.Init(ctx, cfs)
	if err != nil {
		return nil, err
	}
	return &service{
		fs:     afs.New(),
		Config: config,
	}, err
}
