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
	dataURLs    []string
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

//AddHistoryURL adds history ProcessURL
func (r *Response) AddHistoryURL(URL string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.historyURLs = append(r.historyURLs, URL)
}

//AddDataURL adds data ProcessURL
func (r *Response) AddDataURL(URL string) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.dataURLs = append(r.dataURLs, URL)
}

//DataURLs returns data URLs
func (r Response) DataURLs() []string {
	return r.dataURLs
}

//AddError adds repsponse error
func (r *Response) AddError(err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Status = shared.StatusError
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
		dataURLs:    make([]string, 0),
		Errors:      make([]string, 0),
	}
}
