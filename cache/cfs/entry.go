package cfs

import "time"

type Entry struct {
	URL     string
	ModTime time.Time
	Data    []byte
}
