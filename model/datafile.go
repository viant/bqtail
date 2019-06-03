package model

import "time"

type Datafile struct {
	URL       string
	Modified  time.Time
	OnSuccess *Action
	OnFailure *Action
}
