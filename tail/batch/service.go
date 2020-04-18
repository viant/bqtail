package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/config"
	"io/ioutil"
	"path"
	"strings"
)

//Service representa a batch service
type Service interface {
	//Try to acquire batch window
	TryAcquireWindow(ctx context.Context, process *stage.Process, rule *config.Rule) (*Info, error)

	//MatchWindowDataURLs returns matching data URLs
	MatchWindowDataURLs(ctx context.Context, rule *config.Rule, window *Window) error
}

type service struct {
	batchURLProvider func(rule *config.Rule) string
	fs               afs.Service
}


//addLocationFile tracks parent locations for a batch
func (s *service) addLocationFile(ctx context.Context, window *Window, location string) error {
	locationFile := fmt.Sprintf("%v%v", base.Hash(location), shared.LocationExt)
	URL := strings.Replace(window.URL, shared.WindowExt, "/"+locationFile, 1)
	if ok, _ := s.fs.Exists(ctx, URL, option.NewObjectKind(true)); ok {
		return nil
	}
	err := s.fs.Upload(ctx, URL, file.DefaultDirOsMode, strings.NewReader(location), option.NewGeneration(true, 0))
	if isPreConditionError(err) || isRateError(err) {
		err = nil
	}
	return nil
}



//TryAcquireWindow try to acquire window for batched transfer, only one cloud function can acquire window
func (s *service) TryAcquireWindow(ctx context.Context, process *stage.Process, rule *config.Rule) (info *Info, err error) {
	err = base.RunWithRetries(func() error {
		info, err = s.tryAcquireWindow(ctx, process, rule)
		return err
	})
	return info, err

}

//TryAcquireWindow try to acquire window for batched transfer, only one cloud function can acquire window
func (s *service) tryAcquireWindow(ctx context.Context, process *stage.Process, rule *config.Rule) (*Info, error) {
	parentURL, _ := url.Split(process.Source.URL, gs.Scheme)
	windowDest := process.DestTable
	ext := path.Ext(process.Source.URL)
	suffixRaw := process.DestTable + rule.When.Suffix + ext

	if !rule.Batch.MultiPath {
		suffixRaw += parentURL
	}
	windowDest = fmt.Sprintf("%v_%v", process.DestTable, base.Hash(suffixRaw))
	taskURL := s.batchURLProvider(rule)
	batch := rule.Batch
	windowURL := batch.WindowURL(taskURL, windowDest, process.Source.Time)
	exists, _ := s.fs.Exists(ctx, windowURL, option.NewObjectKind(true))

	endTime := batch.WindowEndTime(process.Source.Time)
	startTime := endTime.Add(-batch.Window.Duration)
	var err error
	var window *Window
	if exists {
		window = NewWindow(process, startTime, endTime, windowURL)
		if rule.Batch.MultiPath {
			err = s.addLocationFile(ctx, window, parentURL)
		}
		return &Info{OwnerEventID: window.EventID, WindowURL: windowURL}, err
	}

	if batch.RollOver && !batch.IsWithinFirstHalf(process.Source.Time) {
		prevWindowURL := batch.WindowURL(taskURL, windowDest, process.Source.Time.Add(-(1 + batch.Window.Duration)))
		if exists, _ := s.fs.Exists(ctx, prevWindowURL, option.NewObjectKind(true)); !exists {
			startTime = startTime.Add(-batch.Window.Duration)
		}
	}
	window = NewWindow(process, startTime, endTime, windowURL)
	windowData, _ := json.Marshal(window)
	err = s.fs.Upload(ctx, windowURL, file.DefaultFileOsMode, bytes.NewReader(windowData), option.NewGeneration(true, 0))

	//If file does exists by  Exists operation, try to upload batch file,
	//if there is a race condition ignore precondition or rate limit it means batch file exists, - ignore error and quite
	if isPreConditionError(err) || isRateError(err) {
		window := NewWindow(process, startTime, endTime, windowURL)
		if rule.Batch.MultiPath {
			if err = s.addLocationFile(ctx, window, parentURL); err != nil {
				return nil, err
			}
		}
		return &Info{OwnerEventID: window.EventID, WindowURL: windowURL}, nil
	}
	if rule.Batch.MultiPath {
		err = s.addLocationFile(ctx, window, parentURL)
	}
	return &Info{Window: window}, err
}

func (s *service) readLocation(ctx context.Context, URL string) (string, error) {
	reader, err := s.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (s *service) getBaseURLS(ctx context.Context, rule *config.Rule, window *Window) ([]string, error) {
	var baseURLs = make(map[string]bool)
	baseURL, _ := url.Split(window.Source.URL, gs.Scheme)
	baseURLs[baseURL] = true

	if rule.Batch.MultiPath {
		window.Locations = make([]string, 0)
		URL := strings.Replace(window.URL, shared.WindowExt, "/", 1)
		objects, err := s.fs.List(ctx, URL)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if object.IsDir() || path.Ext(object.Name()) != shared.LocationExt {
				continue
			}
			location, err := s.readLocation(ctx, object.URL())
			if err != nil {
				return nil, errors.Wrapf(err, "failed to load location: %v", object.URL())
			}
			window.Locations = append(window.Locations, object.URL())
			baseURLs[location] = true
		}
	}
	var result = make([]string, 0)
	for k := range baseURLs {
		result = append(result, k)
	}
	return result, nil
}

//MatchWindowData matches window data, it waits for window to ends if needed
func (s *service) MatchWindowDataURLs(ctx context.Context, rule *config.Rule, window *Window) (err error) {
	before := window.End          //inclusive
	after := window.Start.Add(-1) //exclusive
	modFilter := matcher.NewModification(&before, &after)
	var baseURLS []string
	err = base.RunWithRetries(func() error {
		baseURLS, err = s.getBaseURLS(ctx, rule, window)
		return err
	})
	if err != nil {
		return errors.Wrapf(err, "failed get batch location: %v", window.URL)
	}
	var result = make([]string, 0)
	for _, baseURL := range baseURLS {
		if err := s.matchData(ctx, window, rule, baseURL, modFilter, &result); err != nil {
			return err
		}
	}
	window.URIs = result
	return nil
}

func (s *service) matchData(ctx context.Context, window *Window, rule *config.Rule, baseURL string, matcher option.Matcher, result *[]string) error {

	objects, err := s.fs.List(ctx, baseURL)
	if err != nil {
		return errors.Wrapf(err, "failed to list batch %v data files", baseURL)
	}
	for _, object := range objects {
		if rule.HasMatch(object.URL()) {
			source := stage.NewSource(object.URL(), object.ModTime())
			table, err := rule.Dest.ExpandTable(rule.Dest.Table, source)
			if err != nil {
				return errors.Wrapf(err, "failed to expand table: %v", rule.Dest.Table)
			}
			if table != window.DestTable {
				continue
			}
			if object.ModTime().After(window.End) || object.ModTime().Equal(window.End) {
				continue
			}
			if object.ModTime().Before(window.Start) {
				continue
			}
			*result = append(*result, object.URL())
		}
	}
	return nil
}

//New create stage service
func New(batchURLProvider func(rule *config.Rule) string, storageService afs.Service) Service {
	return &service{
		batchURLProvider: batchURLProvider,
		fs:               storageService,
	}
}
