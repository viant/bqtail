package contract

import (
	"bqtail/base"
	"bqtail/stage"
	"bqtail/tail/config"
	"bqtail/tail/status"
)

//Response represents a response
type Response struct {
	base.Response
	status.URIs
	Rule        *config.Rule `json:",omitempty"`
	RuleCount   int
	Destination string `json:",omitempty"`

	JobID           string `json:",omitempty"`
	Batched         bool   `json:",omitempty"`
	BatchRunner     bool   `json:",omitempty"`
	BatchingEventID string `json:",omitempty"`

	TriggerURL   string
	ScheduledURL string      `json:",omitempty"`
	Window       interface{} `json:",omitempty"`

	Info           *stage.Info `json:",omitempty"`
	ListOpCount    int         `json:",omitempty"`
	StorageRetries map[int]int `json:",omitempty"`
	Retriable      bool        `json:",omitempty"`
	RetryError     string      `json:",omitempty"`
	RetryCount     int         `json:",omitempty"`
	MoveError      string      `json:",omitempty"`
	CounterError   string      `json:",omitempty"`
	DownloadError  string      `json:",omitempty"`
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
