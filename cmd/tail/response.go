package tail

import (
	"github.com/viant/bqtail/shared"
	"sync"
	"sync/atomic"
)

//Response represents a response
type Response struct {
	Status      string
	Info        Performance
	Errors      []string
	historyURLs []string
	pending     int32
	mux         sync.Mutex
}

//Performance response info
type Performance struct {
	Published int32
	Batched   int32
	NoMatched int32
	Loaded    int32
	Uplodaded int32
}

//History returns events history
func (r Response) HistoryURLs() []string {
	return r.historyURLs
}

//AddHistoryURL adds history URL
func (r *Response) AddHistoryURL(URL string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.historyURLs = append(r.historyURLs, URL)
}

//AddError adds repsponse error
func (r *Response) AddError(err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Status = shared.StatusError
	shared.LogF("[Error]: %v\n", err)
	r.Errors = append(r.Errors, err.Error())
}

//IncrementPending increments pending
func (r *Response) IncrementPending(deleta int32) {
	atomic.AddInt32(&r.pending, deleta)
}

//Pending returns pending events
func (r Response) Pending() int {
	return int(atomic.LoadInt32(&r.pending))
}

//NewResponse creates a response
func NewResponse() *Response {
	return &Response{
		Status:      shared.StatusOK,
		historyURLs: make([]string, 0),
		Errors:      make([]string, 0),
	}
}
