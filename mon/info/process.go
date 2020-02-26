package info

import "time"

//Process represents a process file
type Process struct {
	URL string `json:",omitempty"`
	Created time.Time `json:",omitempty"`
	Age string `json:",omitempty"`
	Error string `json:",omitempty"`
	ActiveDatafiles int `json:",omitempty"`
	StalledDatafiles int `json:",omitempty"`
}

