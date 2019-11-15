package dispatch

import (
	"bqtail/base"
	"bqtail/task"
	"strings"
	"time"
)

//Job represents a dispatch job
type Job struct {
	*base.Job
	URL string
	*task.Actions
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
func NewJob(URL string, job *base.Job, actions *task.Actions) *Job {
	return &Job{
		URL: URL,
		Job: job,
		Actions:actions,
	}
}



//JobID returns job ID for supplied URL
func JobID(baseURL string, URL string) string {
	if len(baseURL) > len(URL) {
		return ""
	}
	encoded := strings.Trim(string(URL[len(baseURL):]), "/")
	encoded = strings.Replace(encoded, ".json", "", 1)
	jobID := base.EncodePathSeparator(encoded)
	return jobID
}


