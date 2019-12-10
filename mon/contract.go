package mon

import (
	"bqtail/base"
)

//Request represents monitoring request
type Request struct {
	IncludeDone bool
}

//Response represents monitoring response
type Response struct {
	Status string
	Error  string `json:",omitempty"`
	*Info
	Dest map[string]*Info
}

//NewResponse create a response
func NewResponse() *Response {
	return &Response{
		Dest:   make(map[string]*Info),
		Status: base.StatusOK,
		Info:   NewInfo(),
	}
}
