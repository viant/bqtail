package stage

import "time"

//Source represents source
type Source struct {
	SourceURL  string    `json:",omitempty"`
	SourceTime time.Time `json:",omitempty"`

}


//NewSource creates a source
func NewSource(url string, modified time.Time) *Source {
	return &Source{
		SourceURL:  url,
		SourceTime: modified,
	}
}