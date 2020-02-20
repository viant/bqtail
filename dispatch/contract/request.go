package contract

import "github.com/viant/bqtail/base"

//Request represents a request
type Request struct {
	EventID   string
	ProjectID string
	JobID     string
	Job       *base.Job
}
