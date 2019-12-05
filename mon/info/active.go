package info

type Activity struct {
	Running int `json:",omitempty"`
	Scheduled int `json:",omitempty"`
	MaxScheduleTime string `json:",omitempty"`
	Stages map[string]int `json:",omitempty"`
}

