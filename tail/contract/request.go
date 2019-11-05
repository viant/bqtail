package contract

import "time"

//Request represents data tail to BigQuery request
type Request struct {
	EventID   string
	SourceURL string
	Started   time.Time
	Attempt   int
}

//NewRequest creates a request
func NewRequest(ID string, URL string, started time.Time) *Request {
	return &Request{
		EventID:   ID,
		SourceURL: URL,
		Started:   started,
	}
}
