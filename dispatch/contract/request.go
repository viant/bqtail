package contract

import "bqtail/base"

//Request represents a request
type Request struct {
	EventID   string
	ProjectID string
	JobID     string
	Job       *base.Job
}
