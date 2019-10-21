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
	Dest         *Destination  `json:",omitempty"`
	When         matcher.Basic `json:",omitempty"`
	Batch        *Batch        `json:",omitempty"`
	task.Actions `json:",omitempty"`
	Info         base.Info `json:",omitempty"`
	Group        string    `json:",omitempty"`
	Override     *bool
}

//IsAppend returns true if appends
func (r *Rule) IsAppend() bool {
	if r.Dest == nil {
		return true
	}
	if r.Override != nil {
		return ! *r.Override
	}
	return r.Dest.WriteDisposition == "" || r.Dest.WriteDisposition == "WRITE_APPEND"
}



//HasMatch returns true if URL matches prefix or suffix
func (r *Rule) HasMatch(URL string) bool {
	location := url.Path(URL)
	parent, name := path.Split(location)
	return r.When.Match(parent, file.NewInfo(name, 0, 0644, time.Now(), false))
}

func (r Rule) Validate() error {
	if r.Dest == nil {
		return fmt.Errorf("dest was empty")
	}
	return r.Dest.Validate()
}
