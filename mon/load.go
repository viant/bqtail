package mon

import (
	"bqtail/base"
	"bqtail/mon/info"
	"bqtail/stage"
	"strings"
	"time"
)

type activeLoads []*load

func (l activeLoads) groupByDest() map[string]*load {
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
	relative = strings.Trim(relative, "/")
	elements := strings.Split(relative, "/")

	if len(elements) > 2 {
		elements  = []string{elements[0], elements[2]}
	}
	encoded := elements[len(elements)-1]
	eventID := strings.Replace(encoded, base.ActionExt, "", 1)
	dest := strings.Trim(elements[len(elements)-2], "/")
	return &load{
		started: modTime,
		eventID: eventID,
		dest:    dest,
	}
}
