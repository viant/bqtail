package base

import (
	"google.golang.org/api/bigquery/v2"
	"time"
)


//Response represents response
type Response struct {
	Status     string
	Error      string
	EventID    string
	Matched    bool
	MatchedURL string
	JobRef     *bigquery.JobReference
	Started    time.Time
	TimeTaken  time.Duration
}

//SetTimeTaken set time taken
func (r *Response) SetTimeTaken(startTime time.Time) {
	r.TimeTaken = time.Now().Sub(startTime)
}

//SetIfError sets non nil error
func (r *Response) SetIfError(err error) {
	if err == nil {
		return
	}
	r.Status = StatusError
	r.Error = err.Error()
}

//NewResponse create a response
func NewResponse(eventID string) *Response {
	return &Response{
		EventID:eventID,
		Status:  StatusOK,
		Started: time.Now(),
	}
}
