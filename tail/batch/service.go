package batch

import (
	"bqtail/base"
	"bqtail/tail/config"
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
)

type Service interface {
	//Try to acquire batch window
	TryAcquireWindow(ctx context.Context, eventId string, source storage.Object, rule *config.Rule) (*BatchedWindow, error)

	//MatchWindowDataURLs returns matching data URLs
	MatchWindowDataURLs(ctx context.Context, rule *config.Rule, window *Window) error
}

type service struct {
	URL string
	fs  afs.Service
}

//TryAcquireWindow try to acquire window for batched transfer, only one cloud function can acquire window
func (s *service) TryAcquireWindow(ctx context.Context, eventId string, source storage.Object, rule *config.Rule) (*BatchedWindow, error) {
	dest, err := rule.Dest.ExpandTable(rule.Dest.Table, source.ModTime(), source.URL())
	if err != nil {
		return nil, err
	}
	sourceParent, _ := url.Split(source.URL(), gs.Scheme)
	//add source parent URL hash in case versiou path match the same dest table
	windowDest := fmt.Sprintf("%v_%v", dest, base.Hash(sourceParent))

	batch := rule.Batch
	windowURL := batch.WindowURL(windowDest, source.ModTime())
	window, _ := GetWindow(ctx, windowURL, s.fs)
	if window != nil {
		return &BatchedWindow{OwnerEventID: window.EventID}, nil
	}

	endTime := batch.WindowEndTime(source.ModTime())
	startTime := endTime.Add(-batch.Window.Duration)

	if batch.RollOver && !batch.IsWithinFirstHalf(source.ModTime()) {
		windowURL := batch.WindowURL(dest, source.ModTime().Add(-(1 + batch.Window.Duration)))
		if previouWindow, _ := GetWindow(ctx, windowURL, s.fs); previouWindow == nil {
			startTime = startTime.Add(-batch.Window.Duration)
		}
	}

	window = NewWindow(eventId, dest, startTime, endTime, source.URL(), source.ModTime(), windowURL, rule)
	windowData, _ := json.Marshal(window)
	err = s.fs.Upload(ctx, windowURL, file.DefaultFileOsMode, bytes.NewReader(windowData), option.NewGeneration(true, 0))
	if err != nil {
		if isPreConditionError(err) || isRateError(err) {
			window, err := GetWindow(ctx, windowURL, s.fs)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to load window: %v", windowURL)
			}
			return &BatchedWindow{OwnerEventID: window.EventID}, nil
		}
	}
	return &BatchedWindow{Window: window}, nil
}

//MatchWindowData matches window data, it waits for window to ends if needed
func (s *service) MatchWindowDataURLs(ctx context.Context, rule *config.Rule, window *Window) error {
	baseURL, _ := url.Split(window.SourceURL, gs.Scheme)
	before := window.End           //inclusive
	afeter := window.Start.Add(-1) //exclusive
	modFilter := matcher.NewModification(&before, &afeter)
	objects, err := s.fs.List(ctx, baseURL, modFilter)
	if err != nil {
		return errors.Wrapf(err, "failed to list batch %v(%) data files", window.EventID, window.URL)
	}
	var result = make([]string, 0)
	for _, object := range objects {
		if rule.HasMatch(object.URL()) {
			table, err := rule.Dest.ExpandTable(rule.Dest.Table, window.SourceTime, window.SourceURL)
			if err != nil {
				return err
			}
			if table != window.Table {
				continue
			}
			if object.ModTime().After(window.End) || object.ModTime().Equal(window.End) {
				continue
			}
			if object.ModTime().Before(window.Start)  {
				continue
			}
			result = append(result, object.URL())
		}
	}
	window.URIs = result
	return nil
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
