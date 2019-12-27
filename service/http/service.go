package http

import (
	"bqtail/task"
)

//Service represents storage service
type Service interface {
	task.Service
}

type service struct{}

//New creates a service
func New() Service {
	return &service{}
}
