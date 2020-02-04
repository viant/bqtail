package config

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/url"
	"path"
	"time"
)

//Rule represent matching resource route
type Rule struct {
	Disabled              bool           `json:",omitempty"`
	Dest                  *Destination   `json:",omitempty"`
	When                  matcher.Basic  `json:",omitempty"`
	Batch                 *Batch         `json:",omitempty"`
	OnSuccess             []*task.Action `json:",omitempty"`
	OnFailure             []*task.Action `json:",omitempty"`
	Async                 bool           `json:",omitempty"`
	Info                  base.Info      `json:",omitempty"`
	Group                 string         `json:",omitempty"`
	StalledThresholdInSec int            `description:"duration after which unprocess file will be flag as error"`
	CorruptedFileURL      string         `json:",omitempty"`
	InvalidSchemaURL      string         `json:",omitempty"`
	CounterURL            string         `json:",omitempty"`
}

//Actions returns a rule actions
func (r *Rule) Actions() *task.Actions {
	result := &task.Actions{
		OnFailure: r.OnFailure,
		OnSuccess: r.OnSuccess,
	}
	result.Async = r.Async
	if r.Info.URL != "" {
		result.RuleURL = r.Info.URL
	}
	return result
}

//IsAppend returns true if appends
func (r *Rule) IsAppend() bool {
	if r.Dest == nil {
		return true
	}
	if r.Dest.Override != nil {
		return !*r.Dest.Override
	}
	return r.Dest.WriteDisposition == "" || r.Dest.WriteDisposition == "WRITE_APPEND"
}

//DestTable returns dest table
func (r *Rule) DestTable(URL string, modTime time.Time) string {
	table, _ := r.Dest.ExpandTable(r.Dest.Table, modTime, URL)
	if table == "" {
		table = r.Dest.Table
	}
	return table
}

//HasMatch returns true if URL matches prefix or suffix
func (r *Rule) HasMatch(URL string) bool {
	location := url.Path(URL)
	parent, name := path.Split(location)
	match := r.When.Match(parent, file.NewInfo(name, 0, 0644, time.Now(), false))
	return match
}

//Validate checks if rule is valid
func (r Rule) Validate() error {
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	return r.Dest.Validate()
}

func (r *Rule) Init(ctx context.Context, fs afs.Service) error {
	actions := r.Actions()
	if r.Dest.Pattern != "" && r.When.Filter == "" {
		r.When.Filter = r.Dest.Pattern
	}
	return actions.Init(ctx, fs)
}
