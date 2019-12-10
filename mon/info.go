package mon

import (
	"bqtail/mon/info"
)

//Info represents load info
type Info struct {
	*info.Destination
	*info.Activity `json:",omitempty"`
	Stalled        map[string]*info.Metric `json:",omitempty"`
}

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
		Stalled:     make(map[string]*info.Metric),
		Destination: &info.Destination{},
	}
}
