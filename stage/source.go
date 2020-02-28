package stage

import (
	"github.com/viant/bqtail/shared"
	"time"
)

//Source represents source
type Source struct {
	URL    string    `json:",omitempty"`
	Time   time.Time `json:",omitempty"`
	Status string    `json:",omitempty"`
}

//NewSource creates a source
func NewSource(url string, modified time.Time) *Source {
	return &Source{
		URL:    url,
		Time:   modified,
		Status: shared.StatusPending,
	}
}
