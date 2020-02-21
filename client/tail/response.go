package tail

import (
	"sync"
	"sync/atomic"
)

//Response represents a response
type Response struct {
	Published int32
	Batches   int32
	NoMatched int32
	Loaded    int32
	Uplodaded int32
	pending   int32
	Errors    []string
	mux       sync.Mutex
}

//AddError adds repsponse error
func (r *Response) AddError(err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	if len(r.Errors) == 0 {
		r.Errors = make([]string, 0)
	}
	r.Errors = append(r.Errors, err.Error())
}

//IncrementPending increments pending
func (r *Response) IncrementPending(deleta int32)  {
	atomic.AddInt32(&r.pending, deleta)
}

//Pending returns pending events
func (r Response) Pending() int {
	return int(atomic.LoadInt32(&r.pending))
}