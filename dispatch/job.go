package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
	"bqtail/task"
	"time"
)

//Job represents a dispatch job
type Job struct {
	*base.Job
	Actions  *task.Actions
	Response *contract.Response
}

//Completed return completion time
func (j Job) Completed() time.Time {
	baseTime := j.Job.Statistics.EndTime
	if baseTime == 0 {
		baseTime = j.Job.Statistics.StartTime
	}
	if baseTime == 0 {
		return time.Now()
	}
	return time.Unix(0, baseTime*int64(time.Millisecond))
}

//NewJob creates a job
func NewJob(job *base.Job, response *contract.Response) *Job {
	return &Job{
		Job:      job,
		Response: response,
	}
}
