package config

import (
	"fmt"
	"time"
)

const (
	MinWindowDuration        = 10 * time.Second
	MaxWindowDuration        = 8 * time.Minute
	DefaultWindowDurationSec = 90
)

//Window represents batching window
type Window struct {
	time.Duration
	DurationInSec int
}

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

func (w *Window) Validate() error {
	if w.Duration < MinWindowDuration {
		return fmt.Errorf("invalid window duration: had: %v, but min allowed: %v", w.DurationInSec, time.Duration(MinWindowDuration))
	}
	if w.Duration < MaxWindowDuration {
		return fmt.Errorf("invalid window duration: had: %v, but max allowed: %v", w.DurationInSec, time.Duration(MaxWindowDuration))
	}
	return nil
}
