package storage

import (
	"github.com/viant/afs"
	"github.com/viant/bqtail/task"
)

//Service represents fs service
type Service interface {
	task.Service
}

type service struct {
	fs afs.Service
}

//New creates fs service
func New(storage afs.Service) Service {
	return &service{fs: storage}
}
