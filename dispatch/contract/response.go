package contract

import (
	"bqtail/base"
	"time"
)

//Response represents response
type Response struct {
	base.Response
	Jobs       *Jobs
	Batched    map[string]time.Time
	BatchCount int
	Cycles     int
	ListTime   string
	GetCount   int
	Errors     []string
	MaxPending *time.Time
	*Performance
}

//Reset reset response
func (r *Response) Reset() {

}

//HasBatch returns true if it has a bach
func (r *Response) HasBatch(URL string) bool {
	r.Jobs.mux.Lock()
	defer r.Jobs.mux.Unlock()
	_, ok := r.Batched[URL]
	return ok
}

//AddBatch add a batch
func (r *Response) AddBatch(URL string, ts time.Time) {
	r.Jobs.mux.Lock()
	defer r.Jobs.mux.Unlock()
	r.Batched[URL] = ts
}

//AddError adds an error
func (r *Response) AddError(err error) {
	if err == nil {
		return
	}
	r.Jobs.mux.Lock()
	defer r.Jobs.mux.Unlock()
	r.Errors = append(r.Errors, err.Error())
	r.SetIfError(err)
}

//NewResponse creates a new response
func NewResponse() *Response {
	return &Response{
		Jobs:        NewJobs(),
		Performance: NewPerformance(),
		Batched:     make(map[string]time.Time),
		Errors:      make([]string, 0),
		Response:    *base.NewResponse(""),
	}
}
