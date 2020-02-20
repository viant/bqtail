package mon

import (
	"github.com/viant/bqtail/mon/info"
	"github.com/viant/bqtail/tail/config"
)

//Info represents monitoring info
type Info struct {
	*info.Destination
	*info.Activity `json:",omitempty"`
	Stalled        info.Metrics `json:",omitempty"`
	Corrupted      *info.Metric `json:",omitempty"`
	InvalidSchema  *info.Metric `json:",omitempty"`
	rule           *config.Rule
}

//Add adds info
func (i *Info) Add(inf *Info) {
	if i.Activity.Running == nil {
		i.Activity.Running = info.NewMetric()
	}
	if inf.Activity.Running != nil {
		i.Activity.Running.Add(inf.Activity.Running, true)
	}

	if i.Activity.Scheduled == nil {
		i.Activity.Scheduled = info.NewMetric()
	}
	if inf.Activity.Scheduled != nil {
		i.Activity.Scheduled.Add(inf.Activity.Scheduled, true)
	}
}

//NewInfo create a info
func NewInfo() *Info {
	return &Info{
		Activity:    &info.Activity{},
		Destination: &info.Destination{},
	}
}
