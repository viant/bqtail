package contract

//Job represents a dispatch job
type Job struct {
	ID  string
	URL string
	Status string
}

//NewJob creates a job
func NewJob(id, URL string, status string) *Job {
	return &Job{
		ID:  id,
		Status: status,
		URL: URL,
	}
}
