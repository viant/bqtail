package batch

import (
	"bqtail/base"
	"bqtail/tail/config"
	"bqtail/tail/contract"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"google.golang.org/api/googleapi"
	"net/http"
	"path"
	"strings"
	"time"
)

type Service interface {
	//Add adds transfer events to batch stage
	Add(context.Context, storage.Object, *contract.Request, *config.Rule) (*Snapshot, error)

	//Try to acquire batch window
	TryAcquireWindow(ctx context.Context, snapshot *Snapshot, rule *config.Rule) (*BatchedWindow, error)

	//MatchWindowData updates the window with the window span matched transfer datafiles
	MatchWindowData(ctx context.Context, window *Window, rule *config.Rule) error
}

type service struct {
	URL string
	fs  afs.Service
}

func (s *service) scheduleURL(source storage.Object, request *contract.Request, rule *config.Rule) (string, error) {
	dest, err := rule.Dest.ExpandTable(rule.Dest.Table, source.ModTime(), source.URL())
	if err != nil {
		return "", err
	}
	baseURL := url.Join(s.URL, path.Join(dest))
	return url.Join(baseURL, request.EventID+transferableExtension), nil
}

//Add adds matched transfer event to batch stage
func (s *service) Add(ctx context.Context, source storage.Object, request *contract.Request, rule *config.Rule) (snapshot *Snapshot, err error) {
	sourceCreated := source.ModTime()
	URL, err := s.scheduleURL(source, request, rule)
	if err != nil {
		return nil, err
	}
	snapshot = &Snapshot{}
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(request.SourceURL), &snapshot.Schedule, option.NewGeneration(true, 0))
	if err != nil {
		if ! isPreConditionError(err) {
			return nil, errors.Errorf("failed create batch trace file: %v", URL)
		}
		snapshot.Schedule, err = s.fs.Object(ctx, URL)
		if err != nil {
			return nil, errors.Errorf("failed to fetch trace file: %v", URL)
		}
		if snapshot.IsDuplicate(sourceCreated, rule.Batch.Window.Duration) {
			return snapshot, nil
		}
		if err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(request.SourceURL), &snapshot.Schedule); err != nil {
			return nil, errors.Errorf("failed recreate batch trace file: %v", URL)
		}
	}
	rangeMin := snapshot.Schedule.ModTime().Add(-(2*rule.Batch.Window.Duration + 1))
	rangeMax := snapshot.Schedule.ModTime().Add(rule.Batch.Window.Duration + 1)
	parentURL, name := url.Split(URL, gs.Scheme)
	modTimeMatcher := matcher.NewModification(&rangeMax, &rangeMin)
	objects, err := s.fs.List(ctx, parentURL, modTimeMatcher)
	if err != nil {
		objects = []storage.Object{}
	}
	return NewSnapshot(source, request.EventID, name, objects, rule.Batch.Window.Duration), err
}

func (s *service) AcquireWindow(ctx context.Context, baseURL string, window *Window) (string, error) {
	URL := window.URL
	data, err := json.Marshal(window)
	if err != nil {
		return "", err
	}
	if base.IsLoggingEnabled() {
		fmt.Printf("Acquired batch: %v %v\n", window.EventID, URL)
	}

	for i := 0; i < base.BatchVicinityCount; i++ {
		attemptURL := url.Join(baseURL, fmt.Sprintf("%v%v", window.End.Add(-1 * time.Duration(i) * time.Second).UnixNano(), windowExtension))
		window, err := getWindow(ctx, attemptURL, s.fs)
		if err == nil {
			return window.EventID, nil
		}
	}
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data), option.NewGeneration(true, 0))
	if err != nil {
		if isPreConditionError(err) || isRateError(err) {
			window, err := getWindow(ctx, URL, s.fs)
			if err != nil {
				return "", errors.Wrapf(err, "failed to load window: %v", URL)
			}
			return window.EventID, nil
		}
	}
	return "", err
}


//TryAcquireWindow try to acquire window for batched transfer, only one cloud function can acquire window
func (s *service) TryAcquireWindow(ctx context.Context, snapshot *Snapshot, rule *config.Rule) (*BatchedWindow, error) {
	batchEventID, _ := snapshot.GetWindowID(ctx, rule.Batch.Window.Duration, s.fs)
	if batchEventID != "" {
		return &BatchedWindow{BatchingEventID: batchEventID}, nil
	}

	dest, err := rule.Dest.ExpandTable(rule.Dest.Table, snapshot.source.ModTime(), snapshot.source.URL())
	if err != nil {
		return nil, err
	}
	baseURL := url.Join(s.URL, path.Join(dest))
	window := NewWindow(baseURL, snapshot, rule)
	if batchEventID, err = s.AcquireWindow(ctx, baseURL, window); err != nil {
		return nil, err
	}
	if batchEventID != "" {
		return &BatchedWindow{BatchingEventID: batchEventID}, nil
	}
	return &BatchedWindow{Window: window}, nil
}

func (s *service) newWindowSnapshot(ctx context.Context, window *Window) *Snapshot {
	windowDuration := window.End.Sub(window.Start)
	rangeMin := window.Start.Add(-(2*windowDuration + 1))
	rangeMax := window.End.Add(1)
	_, name := url.Split(window.ScheduleURL, gs.Scheme)
	modTimeMatcher := matcher.NewModification(&rangeMax, &rangeMin)
	objects, err := s.fs.List(ctx, window.BaseURL, modTimeMatcher)
	if err != nil {
		objects = []storage.Object{}
	}
	return NewSnapshot(nil, window.EventID, name, objects, windowDuration)

}

//MatchWindowData matches window data, it waits for window to ends if needed
func (s *service) MatchWindowData(ctx context.Context, window *Window, rule *config.Rule) (err error) {
	snapshot := s.newWindowSnapshot(ctx, window)
	if owner, _ := snapshot.IsOwner(ctx, window, s.fs); ! owner {
		window.LostOwnership = true
		_ = s.fs.Delete(ctx, window.URL)
		return nil
	}

	tillWindowEnd := window.End.Sub(time.Now()) + time.Second
	if tillWindowEnd > 0 {
		time.Sleep(tillWindowEnd)
	}
	if err = window.loadDatafile(ctx, s.fs); err != nil {
		return err
	}
	if !window.IsOwner() {
		window.LostOwnership = true
		_ = s.fs.Delete(ctx, window.URL)
		return nil
	}
	return nil
}

func windowedMatcher(after, before time.Time, ext string) *matcher.Modification {
	extMatcher, _ := matcher.NewBasic("", ext, "", nil)
	modTimeMatcher := matcher.NewModification(&before, &after, extMatcher.Match)
	return modTimeMatcher
}

func isPreConditionError(err error) bool {
	origin := errors.Cause(err)
	if googleError, ok := origin.(*googleapi.Error); ok && googleError.Code == http.StatusPreconditionFailed {
		return true
	}
	return false
}

func isRateError(err error) bool {
	origin := errors.Cause(err)
	if googleError, ok := origin.(*googleapi.Error); ok && googleError.Code == http.StatusTooManyRequests {
		return true
	}
	return false
}

//New create stage service
func New(batchURL string, storageService afs.Service) Service {
	return &service{
		URL: batchURL,
		fs:  storageService,
	}
}
