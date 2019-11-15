package contract

import (
	"bqtail/base"
)

//Response represents response
type Response struct {
	base.Response
	Errors []string
	Jobs interface{}
}


//AddError add an error
func (r * Response) AddError(err error) {
	if err == nil {
		return
	}
	r.Errors = append(r.Errors, err.Error())
}

//NewResponse creates a new response
func NewResponse() *Response {
	return &Response{
		Errors: make([]string, 0),
		Response: *base.NewResponse(""),
	}
}
