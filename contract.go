package bqtail

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const (
	StatusOK    = "ok"
	StatusError = "error"
)

type Request struct {
	EventID   string
	SourceURL string
}

type Response struct {
	Status    string
	Error     string
	JobRefs   []*bigquery.JobReference
	TimeTaken time.Duration
	Mode      string
}

func (r *Request) JobID(index int) string {
	return fmt.Sprint("bqtail-%v-%05d", r.EventID, index)
}

//SetIfError sets non nil error
func (r *Response) SetIfError(err error) {
	if err == nil {
		return
	}
	r.Status = StatusError
	r.Error = err.Error()
}

func (r *Response) SetTimeTaken(startTime time.Time) {
	r.TimeTaken = time.Now().Sub(startTime)
}

func NewResponse() *Response {
	return &Response{
		Status:  StatusOK,
		JobRefs: make([]*bigquery.JobReference, 0),
	}
}
