package mon

import "time"

//Error represents an error
type Error struct {
	Message string
	URL     string
	Created time.Time
}
