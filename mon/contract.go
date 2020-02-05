package mon

import (
	"bqtail/shared"
	"time"
)

//Request represents monitoring request
type Request struct {
	IncludeDone bool
	Recency     string
	DestPath    string
	DestBucket  string
}

//Response represents monitoring response
type Response struct {
	Status          string
	Error           string `json:",omitempty"`
	UploadError     string `json:",omitempty"`
	PermissionError string `json:",omitempty"`
	SchemaError     string `json:",omitempty"`
	CorruptedError  string `json:",omitempty"`
	Timestamp       time.Time
	*Info
	Dest []*Info
}

//NewResponse create a response
func NewResponse() *Response {
	return &Response{
		Timestamp: time.Now(),
		Dest:      make([]*Info, 0),
		Status:    shared.StatusOK,
		Info:      NewInfo(),
	}
}
