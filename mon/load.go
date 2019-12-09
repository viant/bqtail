package mon

import (
	"bqtail/base"
	"bqtail/mon/info"
	"bqtail/stage"
	"strings"
	"time"
)

type loads []*load

func (l loads) groupByDest() map[string]*load {
	var result = make(map[string]*load)
	for i, ld := range l {
		_, ok := result[ld.dest]
		if !ok {
			result[ld.dest] = l[i]
		}
		result[ld.dest].AddEvent(result[ld.dest].started)
	}
	return result
}

type load struct {
	dest    string
	eventID string
	info.Metric
	started time.Time
}

func parseLoad(baseURL string, URL string, modTime time.Time) *load {
	relative := string(URL[len(baseURL):])
	relative = strings.Replace(relative, stage.PathElementSeparator, "/", len(relative))
	elements := strings.Split(relative, "/")
	eventID := strings.Replace(elements[len(elements)-1], base.ActionExt, "", 1)
	dest := strings.Trim(elements[len(elements)-2], "/")
	return &load{
		started: modTime,
		eventID: eventID,
		dest:    dest,
	}
}
