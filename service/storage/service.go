package storage

import (
	"github.com/viant/afs"
	"bqtail/task"
)

//Service represents storage service
type Service interface {
	task.Service
}


type service struct {
	storage afs.Service
}

//New creates storage service
func New(storage afs.Service) Service {
	return &service{storage:storage}
}