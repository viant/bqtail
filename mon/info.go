package mon

import (
	"bqtail/mon/info"
)

//Info represents load info
type Info struct {
	Destination *info.Destination
	Active      *info.Activity          `json:",omitempty"`
	Completed   map[string]*info.Metric `json:",omitempty"`
	Stalled     map[string]*info.Metric `json:",omitempty"`
}

func (i *Info) Add(inf *Info) {
	if i.Active.Running == nil {
		i.Active.Running = info.NewMetric()
	}
	if inf.Active.Running != nil {
		i.Active.Running.Add(inf.Active.Running)
	}

	if i.Active.Scheduled == nil {
		i.Active.Scheduled = info.NewMetric()
	}
	if inf.Active.Scheduled != nil {
		i.Active.Scheduled.Add(inf.Active.Scheduled)
	}
}

//NewInfo create a info
func NewInfo() *Info {
	return &Info{
		Active:      &info.Activity{},
		Completed:   make(map[string]*info.Metric),
		Stalled:     make(map[string]*info.Metric),
		Destination: &info.Destination{},
	}
}
