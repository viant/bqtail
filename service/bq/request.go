package bq

import (
	"bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

//Request represents base request
type Request struct {
	task.Actions
	ProjectID string
}

func (r Request) jobReference() *bigquery.JobReference {
	return &bigquery.JobReference{
		JobId:     r.JobID,
		ProjectId: r.ProjectID,
	}
}
