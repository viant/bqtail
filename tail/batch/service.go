package batch

import (
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
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
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
	rangeMin := sourceCreated.Add(-(2*rule.Batch.Window.Duration + 1))
	rangeMax := sourceCreated.Add(rule.Batch.Window.Duration + 1)
	URL, err := s.scheduleURL(source, request, rule)
	if err != nil {
		return nil, err
	}
	parentURL, name := url.Split(URL, gs.Scheme)
	modTimeMatcher := matcher.NewModification(&rangeMax, &rangeMin)
	objects, err := s.fs.List(ctx, parentURL, modTimeMatcher)
	if err != nil {
		objects = []storage.Object{}
	}
	snapshot = NewSnapshot(source, request.EventID, name, objects)
	if snapshot.IsDuplicate(sourceCreated, rule.Batch.Window.Duration) {
		return snapshot, nil
	}
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader(request.SourceURL), &snapshot.Schedule)
	if err != nil {
		err = errors.Errorf("failed create batch trace file: %v", URL)
	}
	return snapshot, err
}

func (s *service) AcquireWindow(ctx context.Context, baseURL string, window *Window) error {
	URL := window.URL
	if URL == "" {
		URL = url.Join(baseURL, fmt.Sprintf("%v%v", window.End.UnixNano(), windowExtension))
	}
	data, err := json.Marshal(window)
	if err != nil {
		return err
	}
	err = s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
	return err
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
	return &BatchedWindow{Window: window}, s.AcquireWindow(ctx, baseURL, window)
}

func (s *service) newWindowSnapshot(ctx context.Context, window *Window) *Snapshot {
	windowDuration := window.End.Sub(window.Start)
	rangeMin := window.Start.Add(-(2 * windowDuration + 1))
	rangeMax := window.End.Add(1)
	_, name := url.Split(window.ScheduleURL, gs.Scheme)
	modTimeMatcher := matcher.NewModification(&rangeMax, &rangeMin)
	objects, err := s.fs.List(ctx, window.BaseURL, modTimeMatcher)
	if err != nil {
		objects = []storage.Object{}
	}
	return NewSnapshot(nil, window.EventID, name, objects)

}

//MatchWindowData matches window data, it waits for window to ends if needed
func (s *service) MatchWindowData(ctx context.Context, window *Window, rule *config.Rule) (err error) {
	//add some delay to see all chanes on google storage
	closingBatchWaitTime := 3 * time.Second
	time.Sleep(closingBatchWaitTime)
	closingBatchWaitTime -= closingBatchWaitTime
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

//New create stage service
func New(batchURL string, storageService afs.Service) Service {
	return &service{
		URL: batchURL,
		fs:  storageService,
	}
}
