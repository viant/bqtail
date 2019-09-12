package dispatch

import (
	"bqtail/dispatch/contract"
	"bqtail/task"
	"time"
)

//Job represents a dispatch job
type Job struct {
	*contract.Job
	Actions  *task.Actions
	Response *contract.Response
	run      []*task.Action
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

//ToRun returns actions to run
func (j Job) ToRun() []*task.Action {
	if len(j.run) > 0 {
		return j.run
	}
	var toRun []*task.Action
	if j.Error() == nil {
		toRun = j.Actions.OnSuccess
	} else {
		toRun = j.Actions.OnFailure
	}
	j.run = toRun
	return toRun
}

//NewJob creates a job
func NewJob(job *contract.Job, response *contract.Response) *Job {
	return &Job{

		Job:      job,
		Response: response,
	}
}
