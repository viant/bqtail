package batch

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"sort"
	"strings"
	"time"
)

//Snapshot represents
type Snapshot struct {
	source         storage.Object
	EventID        string
	Duplicated     bool
	Schedule       storage.Object
	windows        []storage.Object
	dataFiles      []storage.Object
	windowDuration time.Duration
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
func NewSnapshot(source storage.Object, eventID, name string, files [] storage.Object, windowDuration time.Duration) *Snapshot {
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
	sortedWindows := NewObjects(windows, byName)
	sort.Sort(sortedWindows)

	sortedDatafile :=  NewObjects(dataFiles, byModTime)
	sort.Sort(sortedDatafile)
	result := &Snapshot{
		windowDuration: windowDuration,
		EventID:        eventID,
		source:         source,
		Schedule:       schedule,
		windows:        sortedWindows.Elements,
		dataFiles:      sortedDatafile.Elements,
	}
	return result
}
