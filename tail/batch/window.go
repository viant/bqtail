package batch

import (
	"fmt"
	"github.com/viant/afs/url"
	"bqtail/tail/config"
	"bqtail/tail/contract"
	"time"
)

//Window represent batching window
type Window struct {
	URL           string
	Start         time.Time
	End           time.Time
	SourceCreated time.Time
	EventTime     time.Time
	EventID       string
	Datafiles     []*Datafile
}

//NewWindow create a stage batch window
func NewWindow(baseURL string, request *contract.Request, windowStarted time.Time, route *config.Route, sourceCreate time.Time) *Window {
	end := windowStarted.Add(route.Batch.Window.Duration)
	return &Window{
		SourceCreated: sourceCreate,
		URL:           url.Join(baseURL, fmt.Sprintf("%v.win", end.UnixNano())),
		EventID:       request.EventID,
		EventTime:     request.Started,
		Start:         windowStarted,
		End:           end,
	}
}
