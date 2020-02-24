package job

import (
	"encoding/json"
	"github.com/viant/bqtail/stage/activity"
	"google.golang.org/api/bigquery/v2"
	"time"
)

//Logging represents job info summary
type Info struct {
	ProjectID           string
	Error               string
	JobID               string
	CreateTime          time.Time
	StartTime           time.Time
	EndTime             time.Time
	TotalSlotMs         int
	ReservationName     string
	TotalBytesProcessed int
	InputFileBytes      int
	InputFiles          int
	OutputBytes         int
	OutputRows          int
	BadRecords          int
	ExecutionTimeMs     int
	TimeTakenMs         int
	JobType             string
	TempTable           string
	DestinationTable    string
}

//NewInfo creates new job info
func NewInfo(job *bigquery.Job) *Info {
	stageInfo := activity.Parse(job.JobReference.JobId)
	startTime := time.Unix(job.Statistics.StartTime/1000, 0)
	endTime := time.Unix(job.Statistics.EndTime/1000, 0)
	createTime := time.Unix(job.Statistics.CreationTime/1000, 0)
	info := &Info{
		ProjectID:        job.JobReference.ProjectId,
		JobID:            job.Id,
		CreateTime:       createTime,
		StartTime:        startTime,
		EndTime:          endTime,
		TimeTakenMs:      int(job.Statistics.EndTime - job.Statistics.CreationTime),
		ExecutionTimeMs:  int(job.Statistics.EndTime - job.Statistics.StartTime),
		JobType:          stageInfo.Action,
		DestinationTable: stageInfo.DestTable,
	}
	if job.Statistics.Load != nil {
		info.InputFileBytes = int(job.Statistics.Load.InputFileBytes)
		info.InputFiles = int(job.Statistics.Load.InputFiles)
		info.OutputBytes = int(job.Statistics.Load.OutputBytes)
		info.OutputRows = int(job.Statistics.Load.OutputRows)
		info.BadRecords = int(job.Statistics.Load.BadRecords)
	}
	info.TotalBytesProcessed = int(job.Statistics.TotalBytesProcessed)
	info.TotalSlotMs = int(job.Statistics.TotalSlotMs)
	if len(job.Statistics.ReservationUsage) > 0 {
		info.ReservationName = job.Statistics.ReservationUsage[0].Name
	}
	if job.Status != nil && len(job.Status.Errors) > 0 {
		if JSON, err := json.Marshal(job.Status.Errors); err == nil {
			info.Error = string(JSON)
		}
	}
	return info
}
