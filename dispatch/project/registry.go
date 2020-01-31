package project

import (
	"github.com/viant/afs/storage"
)

//Registry represents a project registry
type Registry struct {
	registry map[string]*Events
}

//Events returns
func (r *Registry) Events() []*Events {
	var result = make([]*Events, 0)
	for _, v := range r.registry {
		result = append(result, v)
	}
	return result
}

//Add adds project objects
func (r *Registry) Add(projectID string, event storage.Object) {
	_, ok := r.registry[projectID]
	if !ok {
		r.registry[projectID] = New(projectID)
		r.registry[projectID].ProjectID = projectID
	}
	r.registry[projectID].Items = append(r.registry[projectID].Items, event)
}

//NewRegistry create a registry
func NewRegistry() *Registry {
	return &Registry{registry: make(map[string]*Events)}
}
