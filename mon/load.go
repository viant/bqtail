package mon

import (
	"bqtail/base"
	"bqtail/stage"
	"strings"
	"time"
)

type loads []*load

func (l loads) groupByDest() map[string]*load {
	var result = make(map[string]*load)
	for i, ld := range l {
		_, ok := result[ld.dest]
		if ! ok {
			result[ld.dest] = l[i]
		}
		result[ld.dest].count++
		if ld.started.Before(result[ld.dest].started) {
			result[ld.dest].started = ld.started
		}
	}
	return result
}


type load struct {
	dest    string
	eventID string
	count   int
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
