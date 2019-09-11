package contract

import "bqtail/base"

//Response represents response
type Response struct {
	base.Response
	JobError string
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
