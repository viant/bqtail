package batch

import "time"

//Datafile represents transfer data file
type Datafile struct {
	EventID string
	SourceURL string
	Created time.Time
}
