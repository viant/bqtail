package dispatch

import (
	"google.golang.org/api/bigquery/v2"
	"sync"
)

type jobs struct {
	mutex *sync.Mutex
	byId  map[string]*bigquery.JobListJobs
}

func (j *jobs) put(job *bigquery.JobListJobs) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.byId[job.JobReference.JobId] = job
}

func (j *jobs) get(id string) *bigquery.JobListJobs {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return j.byId[id]
}

func newJobs() *jobs {
	return &jobs{
		mutex: &sync.Mutex{},
		byId:  make(map[string]*bigquery.JobListJobs),
	}
}
