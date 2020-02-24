package contract

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/tail/status"
)

//Response represents a response
type Response struct {
	base.Response
	status.URIs
	RuleCount       int
	Destination     string `json:",omitempty"`
	IsDataFile      bool   `json:",omitempty"`
	JobID           string `json:",omitempty"`
	Batched         bool   `json:",omitempty"`
	BatchRunner     bool   `json:",omitempty"`
	BatchingEventID string `json:",omitempty"`

	TriggerURL     string
	ScheduledURL   string         `json:",omitempty"`
	Window         *batch.Window  `json:",omitempty"`
	Process        *stage.Process `json:",omitempty"`
	ListOpCount    int            `json:",omitempty"`
	StorageRetries map[int]int    `json:",omitempty"`
	Retriable      bool           `json:",omitempty"`
	RetryError     string         `json:",omitempty"`
	RuleError      string         `json:",omitempty"`
	LoadError      string         `json:",omitempty"`
	RetryCount     int            `json:",omitempty"`
	MoveError      string         `json:",omitempty"`
	CounterError   string         `json:",omitempty"`
	DownloadError  string         `json:",omitempty"`
}

//NewResponse creates a new response
func NewResponse(eventID string) *Response {
	return &Response{
		Response: *base.NewResponse(eventID),
	}
}
