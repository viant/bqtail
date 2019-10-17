package contract

import (
	"bqtail/base"
	"bqtail/tail/config"
)

//Response represents a response
type Response struct {
	base.Response
	Rule        *config.Rule `json:",omitempty"`
	Batched     bool         `json:",omitempty"`
	BatchRunner bool         `json:",omitempty"`
	TriggerURL  string
	Window interface{} `json:",omitempty"`
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
