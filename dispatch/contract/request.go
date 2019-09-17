package contract

import "bqtail/base"

type Request struct {
	EventID   string
	ProjectID string
	JobID     string
	Job       *base.Job
}
