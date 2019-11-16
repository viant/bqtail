package contract

import (
	"bqtail/base"
	"sync"
)

//Response represents response
type Response struct {
	base.Response
	Errors []string
	Processes []string
	JobMatched int
	mux *sync.Mutex
	Cycles int
	ListTime string
}


//AddError add an error
func (r * Response) AddError(err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Errors = append(r.Errors, err.Error())
}

//AddProcessed add processed job ID
func (r * Response) AddProcessed(id string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Processes = append(r.Processes, id)
}

//NewResponse creates a new response
func NewResponse() *Response {
	return &Response{
		mux : &sync.Mutex{},
		Errors: make([]string, 0),
		Processes: make([]string, 0),
		Response: *base.NewResponse(""),
	}
}
