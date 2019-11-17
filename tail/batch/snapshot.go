package batch

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"sort"
	"strings"
	"time"
)

//Snapshot represents
type Snapshot struct {
	source     storage.Object
	EventID    string
	Duplicated bool
	Schedule   storage.Object
	windows    []storage.Object
	dataFiles  []storage.Object
}

func (s *Snapshot) IsDuplicate(sourceCreated time.Time, loopbackWindow time.Duration) bool {
	if s.Schedule == nil {
		return false
	}
	duplicateGap := sourceCreated.Sub(s.Schedule.ModTime())
	s.Duplicated = duplicateGap < loopbackWindow
	return s.Duplicated
}

func (s *Snapshot) getMatchingWindows(windowDuration time.Duration) ([]storage.Object, error) {
	var result = make([]storage.Object, 0)
	var err error
	for i := range s.windows {
		windowEnd, e := windowToTime(s.windows[i])
		if e != nil {
			err = e
			continue
		}
		if s.Schedule.ModTime().After(*windowEnd) {
			continue
		}
		windowStart := windowEnd.Add(-windowDuration)
		if s.Schedule.ModTime().Before(windowStart) {
			continue
		}
		result = append(result, s.windows[i])
	}
	if len(result) > 0 {
		err = nil
	}
	for i := range result {
		fmt.Printf("matching window: %v %v\n", result[i].URL(), result[i].ModTime())
	}
	return result, err
}

func (s *Snapshot) IsOwner(ctx context.Context, window *Window, fs afs.Service) (bool, error) {
	duration := window.End.Sub(window.Start)
	windowID, err := s.GetWindowID(ctx, duration, fs)
	if err != nil {
		return false, err
	}
	return windowID == window.EventID, nil
}


func (s *Snapshot) GetWindowID(ctx context.Context, windowDuration time.Duration, fs afs.Service) (string, error) {
	windows, err := s.getMatchingWindows(windowDuration)
	if err != nil {
		return "", err
	}
	if len(windows) == 0 {
		return "", nil
	}
	window, err := getWindow(ctx, windows[0].URL(), fs)
	if err != nil {
		return "", err
	}
	return window.EventID, nil
}


//NewSnapshot creates a new batch snapshot
func NewSnapshot(source storage.Object, eventID, name string, files [] storage.Object) *Snapshot {
	var windows = make([]storage.Object, 0)
	var dataFiles = make([]storage.Object, 0)
	var schedule storage.Object
	for i, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), windowExtension) {
			windows = append(windows, files[i])
			continue
		}
		if files[i].Name() == name {
			schedule = files[i]
		}
		dataFiles = append(dataFiles, files[i])

	}
	sortedwindows := Objects(windows)
	sort.Sort(sortedwindows)
	sortedDatafile := Objects(dataFiles)
	return &Snapshot{
		EventID:   eventID,
		source:    source,
		Schedule:  schedule,
		windows:   sortedwindows,
		dataFiles: sortedDatafile,
	}
}
