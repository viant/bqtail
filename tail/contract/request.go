package contract

import (
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"strings"
	"time"
)

//Request represents data tail to BigQuery request
type Request struct {
	EventID     string
	SourceURL   string
	ScheduleURL string
	Started     time.Time
	Attempt     int
	source *storage.Object
}


//IsLoadAction returns true if action URL
func (r *Request) IsLoadAction(loadActionPrefix string) bool {
	 _, PathURL := url.Base(r.SourceURL, "")
	 return strings.HasPrefix(PathURL, loadActionPrefix)
}

//IsPostLoadAction returns true if deferred task URL
func (r *Request) IsPostLoadAction(taskPrefix string) bool {
	_, PathURL := url.Base(r.SourceURL, "")
	return strings.HasPrefix(PathURL, taskPrefix)
}

//NewRequest creates a request
func NewRequest(ID string, URL string, started time.Time) *Request {
	return &Request{
		EventID:   ID,
		SourceURL: URL,
		Started:   started,
	}
}
