package info

import "time"

//Error represetns an errors
type Error struct {
	Error       string    `json:",omitempty"`
	EventID     string    `json:",omitempty"`
	ProcessURL  string    `json:",omitempty"`
	Destination string    `json:",omitempty"`
	ModTime     time.Time `json:",omitempty"`
	DataURLs    []string  `json:",omitempty"`
}
