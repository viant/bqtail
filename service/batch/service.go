package batch

import (
	"github.com/viant/afs"
	"github.com/viant/bqtail/task"
)

//Service represents fs service
type Service interface {
	task.Service
}

type service struct {
	fs       afs.Service
	registry task.Registry
}

//New creates batch service
func New(fs afs.Service, registry task.Registry) Service {
	return &service{fs: fs, registry: registry}
}
