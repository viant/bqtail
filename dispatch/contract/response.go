package contract

import (
	"github.com/viant/bqtail/base"
	"sync"
	"time"
)

//Response represents response
type Response struct {
	base.Response
	Jobs        *Jobs
	Batched     map[string]time.Time
	BatchCount  int
	Cycles      int
	ListTime    string
	GetCount    int
	Errors      []string
	MaxPending  *time.Time
	Performance ProjectPerformance
	mux         *sync.Mutex
}

func (r *Response) Lock() {
	r.mux.Lock()
}

func (r *Response) UnLock() {
	r.mux.Unlock()
}


//Merge merge performance stats
func (r *Response) Merge(performance *Performance) {
	r.mux.Lock()
	defer r.mux.Unlock()
	_, ok := r.Performance[performance.ProjectID]
	if !ok {
		r.Performance[performance.ProjectID] = performance
		return
	}
	r.Performance[performance.ProjectID].Merge(performance)
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
	r.mux.Lock()
	defer r.mux.Unlock()
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
		Performance: make(map[string]*Performance),
		Batched:     make(map[string]time.Time),
		Errors:      make([]string, 0),
		Response:    *base.NewResponse(""),
		mux:         &sync.Mutex{},
	}
}
