package model

import (
	"google.golang.org/api/bigquery/v2"
	"time"
)

type Meta struct {
	EventID  string
	JobID    string
	From     time.Time
	To       time.Time
	Fileset  []*Datafile
	Requests []*bigquery.JobConfigurationLoad
}
