package project

import (
	"github.com/viant/afs/storage"
	"sync"
)

//Registry represents a project registry
type Registry struct {
	registry map[string]*Events
	mux sync.RWMutex
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
func (r *Registry) Add(regionProject string, event storage.Object) {
	r.mux.Lock()
	defer r.mux.Unlock()
	_, ok := r.registry[regionProject]
	if !ok {
		r.registry[regionProject] = New(regionProject)
	}
	r.registry[regionProject].Items = append(r.registry[regionProject].Items, event)
}


//NewRegistry create a registry
func NewRegistry() *Registry {
	return &Registry{registry: make(map[string]*Events)}
}
