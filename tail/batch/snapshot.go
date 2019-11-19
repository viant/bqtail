package batch

import (
	"bqtail/base"
	"context"
	"encoding/json"
	"fmt"
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






func getMatchingWindows(windows []storage.Object ,windowDuration time.Duration, at time.Time) ([]storage.Object, error) {
	var result = make([]storage.Object, 0)
	var err error
	for i := range windows {
		windowEnd, e := windowToTime(windows[i])
		if e != nil {
			err = e
			continue
		}
		if at.After(*windowEnd) {
			continue
		}
		windowStart := windowEnd.Add(-windowDuration)
		if at.Before(windowStart) {
			continue
		}
		result = append(result, windows[i])
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
	openWindows, err := getMatchingWindows(s.windows,  windowDuration, s.Schedule.ModTime())
	if err != nil {
		return "", err
	}
	if len(openWindows) == 0 {
		return "", nil
	}
	window, err := getWindow(ctx, openWindows[0].URL(), fs)
	if err != nil {
		return "", err
	}
	return window.EventID, nil
}

func asObjectMap(o storage.Object) map[string]interface{} {
	if o == nil {
		return map[string]interface{}{}
	}
	return map[string]interface{}{
		"URL": o.Name(),
		"ModTime":o.ModTime(),
	}
}




func  (s *Snapshot) asWindowObjectMap(o storage.Object) map[string]interface{} {
	result := asObjectMap(o)
	end, _ := windowToTime(o)
	start := end.Add(-s.windowDuration)
	result["Start"] = start
	result["End"] = end
	return result
}



func (s *Snapshot) String() string {
	var windows = make([]map[string]interface{}, 0)
	for i:= range s.windows {
		windows = append(windows, s.asWindowObjectMap(s.windows[i]))
	}
	var details = map[string]interface{}{
		"EventID":s.EventID,
		"Schedule": asObjectMap(s.Schedule),
		"Windows": windows,
	}
	data, _ := json.Marshal(details)
	return string(data)
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
	if base.IsLoggingEnabled() {
		fmt.Printf("NewSnapshot: %v (%v): %v\n", result.EventID, name, result.String())
	}
	return result
}
