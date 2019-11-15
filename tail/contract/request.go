package contract

import (
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
}


//IsAction returns true if action URL
func (r *Request) IsAction(actionPrefix string) bool {
	 _, PathURL := url.Base(r.SourceURL, "")
	 return strings.HasPrefix(PathURL, actionPrefix)
}

//NewRequest creates a request
func NewRequest(ID string, URL string, started time.Time) *Request {
	return &Request{
		EventID:   ID,
		SourceURL: URL,
		Started:   started,
	}
}
