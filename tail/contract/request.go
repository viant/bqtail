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
	source      *storage.Object
}

//HasURLPrefix returns true if source SourceURL has prefix
func (r *Request) HasURLPrefix(prefix string) bool {
	_, PathURL := url.Base(r.SourceURL, "")
	return strings.HasPrefix(PathURL, prefix)
}

//SetServiceRequest creates a request
func NewRequest(ID string, URL string, started time.Time) *Request {
	return &Request{
		EventID:   ID,
		SourceURL: URL,
		Started:   started,
	}
}
