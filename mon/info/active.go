package info

//Activity represents bqtail activity
type Activity struct {
	Running   *Metric            `json:",omitempty"`
	Scheduled *Metric            `json:",omitempty"`
	Done      *Metric            `json:",omitempty"`
	Stages    map[string]*Metric `json:",omitempty"`
}
