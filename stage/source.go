package stage

import "time"

//Source represents source
type Source struct {
	URL  string    `json:",omitempty"`
	Time time.Time `json:",omitempty"`
}

//NewSource creates a source
func NewSource(url string, modified time.Time) *Source {
	return &Source{
		URL:  url,
		Time: modified,
	}
}
