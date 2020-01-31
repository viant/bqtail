package project

import (
	"bqtail/dispatch/contract"
	"github.com/viant/afs/storage"
)

//Events represents objects
type Events struct {
	*contract.Performance
	Items []storage.Object
}

//New creates project events
func New(projectID string) *Events {
	return &Events{
		Performance: contract.NewPerformance(),
		Items:       make([]storage.Object, 0),
	}
}
