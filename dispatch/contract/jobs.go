package contract

import "sync"

//Jobs represents job map
type Jobs struct {
	Jobs map[string]*Job
	mux  *sync.Mutex
}

//Add adds a job to a map
func (j *Jobs) Add(job *Job) {
	j.mux.Lock()
	defer j.mux.Unlock()
	j.Jobs[job.URL] = job
}

//Has returns true if has job URL
func (j *Jobs) Has(URL string) bool {
	j.mux.Lock()
	defer j.mux.Unlock()
	_, ok := j.Jobs[URL]
	return ok
}

//NewJobs create new jobs
func NewJobs() *Jobs {
	return &Jobs{
		Jobs: make(map[string]*Job),
		mux:  &sync.Mutex{},
	}
}
