package batch

import (
	"bqtail/tail/config"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"io/ioutil"
	"sort"
	"strconv"
	"time"
)

type BatchedWindow struct {
	*Window
	BatchingEventID string
}

//Window represent batching window
type Window struct {
	URL           string      `json:",omitempty"`
	Start         time.Time   `json:",omitempty"`
	LostOwnership bool        `json:",omitempty"`
	BaseURL       string      `json:",omitempty"`
	End           time.Time   `json:",omitempty"`
	SourceCreated time.Time   `json:",omitempty"`
	EventID       string      `json:",omitempty"`
	ScheduleURL   string      `json:",omitempty"`
	Datafiles     []*Datafile `json:",omitempty"`
}

func (w *Window) IsOwner() bool {
	if len(w.Datafiles) == 0 {
		return false
	}
	var scheduleDatafiles = make([]*Datafile, 0)
	for i, datafile := range w.Datafiles {
		if datafile.URL == w.ScheduleURL {
			scheduleDatafiles = append(scheduleDatafiles, w.Datafiles[i])
		}
	}
	if len(scheduleDatafiles) == 0 {
		return false
	}
	return scheduleDatafiles[0].EventID == w.EventID
}

func (w *Window) loadDatafile(ctx context.Context, fs afs.Service) error {
	var result = make([]*Datafile, 0)
	eventMatcher := windowedMatcher(w.Start.Add(-1), w.End.Add(1), transferableExtension)
	traceFiles, err := fs.List(ctx, w.BaseURL, eventMatcher)
	if err != nil {
		return err
	}
	sortedTransfers := Objects(traceFiles)
	sort.Sort(sortedTransfers)
	result = make([]*Datafile, 0)
	for i := range sortedTransfers {
		if traceFiles[i].ModTime().Before(w.Start) || traceFiles[i].ModTime().After(w.End) {
			continue
		}
		datafile, e := loadDatafile(ctx, traceFiles[i], fs)
		if e != nil {
			err = e
			continue
		}
		result = append(result, datafile)
	}
	w.Datafiles = result
	return err
}

//NewWindow create a stage batch window
func NewWindow(baseURL string, snapshot *Snapshot, route *config.Rule) *Window {
	end := snapshot.Schedule.ModTime().Add(route.Batch.Window.Duration)
	return &Window{
		BaseURL:       baseURL,
		SourceCreated: snapshot.source.ModTime(),
		URL:           url.Join(baseURL, fmt.Sprintf("%v%v", end.UnixNano(), windowExtension)),
		EventID:       snapshot.EventID,
		Start:         snapshot.Schedule.ModTime(),
		ScheduleURL:   snapshot.Schedule.URL(),
		End:           end,
	}
}

func windowToTime(window storage.Object) (*time.Time, error) {
	result, err := windowNameToTime(window.Name())
	if err != nil {
		return nil, errors.Wrapf(err, "invalid nano time for URL: %v", window.URL())
	}
	return result, nil
}

func windowNameToTime(name string) (*time.Time, error) {
	nanoTime := string(name[:len(name)-4])
	unixNano, err := strconv.ParseInt(nanoTime, 10, 64)
	if err != nil {
		return nil, err
	}
	result := time.Unix(0, unixNano)
	return &result, nil
}

func getWindow(ctx context.Context, URL string, fs afs.Service) (*Window, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read window: %v", URL)
	}
	window := &Window{}
	return window, json.Unmarshal(data, window)
}
