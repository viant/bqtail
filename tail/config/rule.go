package config

import (
	"bqtail/base"
	"bqtail/task"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/url"
	"path"
	"time"
)

//Rule represent matching resource route
type Rule struct {
	Disabled              bool          `json:",omitempty"`
	Dest                  *Destination  `json:",omitempty"`
	When                  matcher.Basic `json:",omitempty"`
	Batch                 *Batch        `json:",omitempty"`
	task.Actions          `json:",omitempty"`
	Info                  base.Info `json:",omitempty"`
	Group                 string    `json:",omitempty"`
	StalledThresholdInSec int       `description:"duration after which unprocess file will be flag as error"`
	CorruptedFileURL      string    `json:",omitempty"`
	InvalidSchemaURL      string    `json:",omitempty"`
	CounterURL            string    `json:",omitempty"`
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
	return r.When.Match(parent, file.NewInfo(name, 0, 0644, time.Now(), false))
}

//Validate checks if rule is valid
func (r Rule) Validate() error {
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	return r.Dest.Validate()
}
