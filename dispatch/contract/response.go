package contract

import (
	"bqtail/base"
)

//Response represents response
type Response struct {
	base.Response
	Errors []string
	Processes []string
	JobMatched int
}


//AddError add an error
func (r * Response) AddError(err error) {
	if err == nil {
		return
	}
	r.Errors = append(r.Errors, err.Error())
}

//AddProcessed add processed job ID
func (r * Response) AddProcessed(id string) {
	r.Processes = append(r.Processes, id)
}

//NewResponse creates a new response
func NewResponse() *Response {
	return &Response{
		Errors: make([]string, 0),
		Processes: make([]string, 0),
		Response: *base.NewResponse(""),
	}
}
