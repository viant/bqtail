package batch

import "time"

//Datafile represents transfer data file
type Datafile struct {
	URL       string
	EventID   string
	SourceURL string
	Created   time.Time
}
