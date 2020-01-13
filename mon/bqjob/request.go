package colector

import (
	"time"
)

type Request struct {
	MinTime   time.Time
	MaxTime   time.Time
	ProjectID string
	DestURL   string
}
