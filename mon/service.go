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
	"github.com/viant/afs/url"
	_ "github.com/viant/afsc/gs"
	"path"
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

func (s *service) check(ctx context.Context, request *Request, response *Response) error {
	err := request.Validate()
	if err != nil {
		return err
	}
	infoDest := map[string]*Info{}
	active, err := s.getActiveLoads(ctx)
	if err != nil {
		return err
	}
	s.updateActiveLoads(active, infoDest)

	schedules, err := s.getScheduledBatches(ctx)
	if err != nil {
		return err
	}
	s.updateBatches(schedules, infoDest)

	stages, err := s.getLoadStages(ctx)
	if err != nil {
		return err
	}
	s.updateStages(stages, infoDest)
	for k, inf := range infoDest {
		response.Info.Add(inf)
		if time.Now().Sub(inf.Active.Running.Min) > time.Hour {
			if _, has := response.Stalled[inf.Destination.Table]; !has {
				response.Stalled[inf.Destination.Table] = info.NewMetric()
			}
			response.Status = base.StatusStalled
			response.Stalled[inf.Destination.Table].AddEvent(inf.Active.Running.Min)
		}
		response.ByDestination = append(response.ByDestination, infoDest[k])
	}
	return nil
}

func (s *service) updateActiveLoads(loadsInfo loads, infoDest map[string]*Info) {
	for k, v := range loadsInfo.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Active.Running == nil {
			inf.Active.Running = info.NewMetric()
		}
		inf.Active.Running.Add(&v.Metric)
		infoDest[inf.Destination.Table] = inf
	}
}

func (s *service) updateBatches(batchSlice batches, infoDest map[string]*Info) {
	for k, v := range batchSlice.groupByDest() {
		inf := s.getInfo(k, infoDest)
		if inf.Active.Scheduled == nil {
			inf.Active.Scheduled = info.NewMetric()
		}
		inf.Active.Scheduled.Add(&v.Metric)
		infoDest[inf.Destination.Table] = inf
	}
}

func (s *service) updateStages(stages []*stage.Info, infoDest map[string]*Info) {
	for _, stageInfo := range stages {
		inf := s.getInfo(stageInfo.DestTable, infoDest)
		if len(inf.Active.Stages) == 0 {
			inf.Active.Stages = make(map[string]*info.Metric)
		}
		stageKey := fmt.Sprintf("%04d-%v", stageInfo.Sequence(), stageInfo.Action)
		stageValue, ok := inf.Active.Stages[stageKey]
		if !ok {
			stageValue = info.NewMetric()
		}
		stageValue.AddEvent(stageInfo.SourceTime)
		inf.Active.Stages[stageKey] = stageValue
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
		if object.IsDir() {
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

func (s *service) getActiveLoads(ctx context.Context) (loads, error) {
	result := loads{}
	err := s.listActiveLoads(ctx, s.Config.ActiveLoadURL, &result)
	return result, err
}

func (s *service) listActiveLoads(ctx context.Context, URL string, result *loads) error {
	objects, err := s.fs.List(ctx, URL)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if url.Equals(object.URL(), URL) {
			continue
		}
		if object.IsDir() {
			if err = s.listActiveLoads(ctx, object.URL(), result); err != nil {
				return err
			}
			continue
		}
		*result = append(*result, parseLoad(s.ActiveLoadURL, object.URL(), object.ModTime()))

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
