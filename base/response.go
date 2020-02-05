package base

import (
	"bqtail/shared"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Response represents response
type Response struct {
	Status        string
	Error         string `json:",omitempty"`
	NotFoundError string `json:",omitempty"`
	MoveError     string `json:",omitempty"`

	UploadError string                 `json:",omitempty"`
	EventID     string                 `json:",omitempty"`
	Matched     bool                   `json:",omitempty"`
	MatchedURL  string                 `json:",omitempty"`
	JobRef      *bigquery.JobReference `json:",omitempty"`
	Started     time.Time
	TimeTakenMs int
}

//SetTimeTaken set time taken
func (r *Response) SetTimeTaken(startTime time.Time) {
	r.TimeTakenMs = int(time.Now().Sub(startTime) / time.Millisecond)
}

//SetIfError sets non nil error
func (r *Response) SetIfError(err error) {
	if err == nil {
		return
	}
	r.Status = shared.StatusError
	r.Error = err.Error()
}

//NewResponse create a response
func NewResponse(eventID string) *Response {
	return &Response{
		EventID: eventID,
		Status:  shared.StatusOK,
		Started: time.Now(),
	}
}
