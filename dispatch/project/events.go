package project

import (
	"github.com/viant/bqtail/dispatch/contract"
	"github.com/viant/afs/storage"
	"strings"
)

//Events represents objects
type Events struct {
	*contract.Performance
	Items []storage.Object
}

//New creates project events
func New(regionProject string) *Events {
	result := &Events{
		Performance: contract.NewPerformance(),
		Items:       make([]storage.Object, 0),
	}
	parts := strings.Split(regionProject, ":")
	result.ProjectID = regionProject
	if len(parts) >= 2 {
		result.ProjectID = parts[0]
		result.Region = parts[1]
	}
	return result
}
