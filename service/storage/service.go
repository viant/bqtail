package storage

import (
	"github.com/viant/bqtail/task"
	"github.com/viant/afs"
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
