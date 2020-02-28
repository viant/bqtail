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

//Put adds project objects
func (r *Registry) Add(regionedProject string, event storage.Object) {
	_, ok := r.registry[regionedProject]
	if !ok {
		r.registry[regionedProject] = New(regionedProject)
	}
	r.registry[regionedProject].Items = append(r.registry[regionedProject].Items, event)
}

//NewRegistry create a registry
func NewRegistry() *Registry {
	return &Registry{registry: make(map[string]*Events)}
}
