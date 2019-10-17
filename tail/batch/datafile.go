package batch

import "time"

//Datafile represents transfer data file
type Datafile struct {
	URL       string `json:",omitempty"`
	EventID   string `json:",omitempty"`
	SourceURL string `json:",omitempty"`
	Created   time.Time `json:",omitempty"`
}
