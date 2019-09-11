package contract

import "bqtail/base"

//Response represents a response
type Response struct {
	base.Response
	Batched     bool
	BatchRunner bool
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
