package contract

import (
	"bqtail/base"
	"time"
)

//Response represents response
type Response struct {
	base.Response
	Jobs         *Jobs
	Batched      map[string]time.Time
	BatchCount   int
	Cycles       int
	ListTime     string
	GetCount     int
	Errors       []string
	*Performance
}

func (r *Response) Reset() {
	r.BatchCount = 0
}

func (r *Response) HasBatch(URL string) bool {
	r.Jobs.mux.Lock()
	defer r.Jobs.mux.Unlock()
	_, ok := r.Batched[URL]
	return ok
}

func (r *Response) AddBatch(URL string, ts time.Time) {
	r.Jobs.mux.Lock()
	defer r.Jobs.mux.Unlock()
	r.Batched[URL] = ts
}

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
		Jobs:     NewJobs(),
		Performance: NewPerformance(),
		Batched:  make(map[string]time.Time),
		Errors:   make([]string, 0),
		Response: *base.NewResponse(""),
	}
}
