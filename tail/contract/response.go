package contract

import (
	"bqtail/base"
	"bqtail/tail/config"
	"bqtail/task"
)

//Response represents a response
type Response struct {
	base.Response
	Rule            *config.Rule `json:",omitempty"`
	RuleCount       int
	Destination     string `json:",omitempty"`
	JobID           string `json:",omitempty"`
	Batched         bool   `json:",omitempty"`
	BatchRunner     bool   `json:",omitempty"`
	BatchingEventID string `json:",omitempty"`
	TriggerURL      string
	ScheduledURL    string         `json:",omitempty"`
	Window          interface{}    `json:",omitempty"`
	Actions         []*task.Action `json:",omitempty"`
	Corrupted       []string       `json:",omitempty"`
	Missing         []string       `json:",omitempty"`
	ListOpCount     int            `json:",omitempty"`
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
