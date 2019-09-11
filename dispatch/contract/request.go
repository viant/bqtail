package contract

type Request struct {
	EventID string
	ProjectID string
	JobID string
	Job *Job
}
