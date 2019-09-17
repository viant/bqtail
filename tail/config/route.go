package config

import (
	"bqtail/task"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/url"
	"path"
	"time"
)

//Route represent matching resource route
type Route struct {
	Dest  *Destination
	When  matcher.Basic
	Batch *Batch
	task.Actions
}

//HasMatch returns true if URL matches prefix or suffix
func (r *Route) HasMatch(URL string) bool {
	location := url.Path(URL)
	parent, name := path.Split(location)
	return r.When.Match(parent, file.NewInfo(name, 0, 0644, time.Now(), false))
}

func (r Route) Validate() error {
	if r.Dest == nil {
		return fmt.Errorf("Dest was empty")
	}
	return r.Dest.Validate()
}
