package mon

import (
	"github.com/viant/bqtail/mon/info"
	"github.com/viant/bqtail/shared"
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
	URL     string
	dest    string
	eventID string
	info.Metric
	started time.Time
}

func (l load) ErrorURL() string {
	errorURL := l.URL
	errorURL = strings.Replace(errorURL, "Journal/Running", "error", 1)
	errorURL = strings.Replace(errorURL, ".run", ".err", 1)
	errorURL = strings.Replace(errorURL, "--", "/", strings.Count(errorURL, "--"))
	return errorURL
}

func parseLoad(baseURL string, URL string, modTime time.Time) *load {
	relative := string(URL[len(baseURL):])
	relative = strings.Replace(relative, shared.PathElementSeparator, "/", len(relative))
	relative = strings.Trim(relative, "/")
	elements := strings.Split(relative, "/")

	if len(elements) > 2 {
		elements = []string{elements[0], elements[2]}
	}
	encoded := elements[len(elements)-1]
	eventID := strings.Replace(encoded, shared.ProcessExt, "", 1)
	dest := strings.Trim(elements[len(elements)-2], "/")
	return &load{
		URL:     URL,
		started: modTime,
		eventID: eventID,
		dest:    dest,
	}
}
