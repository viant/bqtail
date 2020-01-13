package colector

import "time"

type Info struct {
	ProjectID string
	JobID string
	StartTime time.Time
	EndTime time.Time
	TimeTakenMs int
	JobType string
	TempTable string
	DestinationTable string
}