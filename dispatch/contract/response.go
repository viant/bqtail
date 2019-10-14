package contract

import (
	"bqtail/base"
	"bqtail/dispatch/config"
)

//Response represents response
type Response struct {
	base.Response
	Rule     *config.Rule `json:",omitempty"`
	JobError string       `json:",omitempty"`
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
