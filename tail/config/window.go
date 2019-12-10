package config

import (
	"fmt"
	"time"
)

const (
	//MinWindowDuration min window duration
	MinWindowDuration = 10 * time.Second
	//DefaultWindowDurationSec default window duration
	DefaultWindowDurationSec = 95
)

//Window represents batching window
type Window struct {
	time.Duration
	DurationInSec int
}

//Init initialises window
func (w *Window) Init() {
	if w.DurationInSec == 0 && w.Duration == 0 {
		w.DurationInSec = DefaultWindowDurationSec
	}
	if w.DurationInSec > 0 {
		w.Duration = time.Second * time.Duration(w.DurationInSec)
	}
	if w.DurationInSec == 0 && w.Duration > 0 {
		w.DurationInSec = int(w.Duration / time.Second)
	}
}

//Validate checks if window is valid
func (w *Window) Validate() error {
	if w.Duration < MinWindowDuration {
		return fmt.Errorf("invalid window duration: had: %v, but min allowed: %v", w.DurationInSec, time.Duration(MinWindowDuration))
	}
	return nil
}
