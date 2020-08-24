package config

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/task"
	"path"
	"strings"
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
	MaxReload             *int           `json:",omitempty"`
}

//StalledDuration returns stalled duration
func (r Rule) StalledDuration() time.Duration {
	if r.StalledThresholdInSec == 0 {
		return shared.StalledDuration
	}
	return time.Second * time.Duration(r.StalledThresholdInSec)

}

//MaxReloadAttempts returns max reload attempts
func (r *Rule) MaxReloadAttempts() int {
	if r.MaxReload == nil {
		r.MaxReload = &shared.MaxReload
	}
	return *r.MaxReload
}

//Actions returns a rule actions
func (r *Rule) Actions() *task.Actions {
	result := &task.Actions{
		OnFailure: r.OnFailure,
		OnSuccess: r.OnSuccess,
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

//IsDMLCopy returns true if dml append flag is true
func (r *Rule) IsDMLCopy() bool {
	if r.Dest == nil || r.Dest.Transient == nil {
		return false
	}
	if r.Dest.Transient.CopyMethod == nil {
		return false
	}
	return strings.ToUpper(*r.Dest.Transient.CopyMethod) == shared.CopyMethodDML
}

//DestTable returns dest table
func (r *Rule) DestTable(URL string, modTime time.Time) string {
	table, _ := r.Dest.ExpandTable(r.Dest.Table, stage.NewSource(URL, modTime))
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

//Init initialises rule
func (r *Rule) Init(ctx context.Context, fs afs.Service) error {
	actions := r.Actions()
	if r.Dest.Pattern != "" && r.When.Filter == "" {
		r.When.Filter = r.Dest.Pattern
	}
	err := actions.Init(ctx, fs)
	return err
}
