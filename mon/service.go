package mon

import (
	"bqtail/base"
	"bqtail/mon/info"
	"bqtail/stage"
	"bqtail/tail"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	//add gcs storage API
	_ "github.com/viant/afsc/gs"
	"path"
	"sort"
	"sync"
	"time"
)

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
	waitGroup.Add(4)
	infoDest := map[string]*Info{}

	var active, doneLoads activeLoads
	var schedules batches
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
	for k, inf := range infoDest {
		response.Info.Add(inf)
		if inf.Activity.Running != nil {
			if time.Now().Sub(inf.Activity.Running.Min) > time.Hour {
				if _, has := response.Stalled[inf.Destination.Table]; !has {
					response.Stalled[inf.Destination.Table] = info.NewMetric()
				}
				response.Status = base.StatusStalled
				response.Stalled[inf.Destination.Table].AddEvent(inf.Activity.Running.Min)
			}
		}
		response.Dest[k] = infoDest[k]

	}
	return nil
}

func (s *service) updateActiveLoads(loadsInfo activeLoads, infoDest map[string]*Info) {
	for k, v := range loadsInfo.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Running == nil {
			inf.Activity.Running = info.NewMetric()
		}
		inf.Activity.Running.Add(&v.Metric, true)
		infoDest[inf.Destination.Table] = inf
	}
}

func (s *service) updateRecentlyDone(recentLoads activeLoads, infoDest map[string]*Info) {
	for k, v := range recentLoads.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Done == nil {
			inf.Activity.Done = info.NewMetric()
		}
		inf.Activity.Done.Add(&v.Metric, false)
		infoDest[inf.Destination.Table] = inf
	}
}

func (s *service) updateBatches(batchSlice batches, infoDest map[string]*Info) {
	for k, v := range batchSlice.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Activity.Scheduled == nil {
			inf.Activity.Scheduled = info.NewMetric()
		}
		inf.Activity.Scheduled.Add(&v.Metric, true)
		infoDest[inf.Destination.Table] = inf
	}
}

func (s *service) updateStages(stages []*stage.Info, infoDest map[string]*Info) {
	for _, stageInfo := range stages {
		inf := s.getInfo(stageInfo.DestTable, infoDest)

		if len(inf.Activity.Stages) == 0 {
			inf.Activity.Stages = make(map[string]*info.Metric)
		}
		stageKey := fmt.Sprintf("%04d-%v", stageInfo.Sequence(), stageInfo.Action)
		stageValue, ok := inf.Activity.Stages[stageKey]
		if !ok {
			stageValue = info.NewMetric()
		}
		stageValue.AddEvent(stageInfo.SourceTime)
		inf.Activity.Stages[stageKey] = stageValue
		infoDest[inf.Destination.Table] = inf
	}
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
	}
	return inf
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
	err := s.listLoadJobs(ctx, s.Config.ActiveLoadJobURL, s.Config.ActiveLoadJobURL, &result)
	return result, err
}

func (s *service) getRecentlyDoneLoads(ctx context.Context) (activeLoads, error) {
	result := activeLoads{}
	err := s.listDoneLoads(ctx, s.Config.DoneLoadJobURL, &result)
	return result, err
}

func (s *service) listLoadJobs(ctx context.Context, baseURL, URL string, result *activeLoads) error {
	objects, err := s.fs.List(ctx, URL)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if url.Equals(object.URL(), URL) {
			continue
		}
		if object.IsDir() {
			if err = s.listLoadJobs(ctx, baseURL, object.URL(), result); err != nil {
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
	if err != nil {
		return err
	}

	var destLocations = make(map[string]storage.Object)
	sortedTables := NewObjects(objects[1:], byModTime)
	sort.Sort(sortedTables)

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
		sortedHours := NewObjects(hourDone[1:], byModTime)
		sort.Sort(sortedHours)
		if len(sortedHours.Elements) == 0 {
			continue
		}
		recentHour := sortedHours.Elements[len(sortedHours.Elements)-1]
		if err = s.listLoadJobs(ctx, baseURL, recentHour.URL(), result); err != nil {
			return err
		}
	}
	return nil
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
