package contract

import (
	"bqtail/base"
)

//Response represents response
type Response struct {
	base.Response
	Jobs         *Jobs
	Cycles       int
	ListTime     string
	ListCount    int
	GetCount     int
	Errors       []string
	MissingCount int32
	RunningCount int32
	PendingCount int32
}

func (r *Response) Reset() {
	r.PendingCount = 0
	r.RunningCount = 0
	r.MissingCount = 0
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
		Errors:   make([]string, 0),
		Response: *base.NewResponse(""),
	}
}
