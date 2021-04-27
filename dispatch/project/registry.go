package project

import (
	"github.com/viant/afs/storage"
	"sync"
)

//Registry represents a project registry
type Registry struct {
	registry map[string]*Events
	mux      sync.RWMutex
	ScheduleBatches
}

type ScheduleBatches struct {
	Scheduled map[string]storage.Object
	batches   map[string]bool
}

func (s *ScheduleBatches) HasSchedule(URL string) bool {
	_, ok := s.Scheduled[URL]
	return ok
}

func (s *ScheduleBatches) HasBatch(URL string) bool {
	_, ok := s.batches[URL]
	return ok
}

//Events returns
func (r *Registry) Events() []*Events {
	var result = make([]*Events, 0)
	for _, v := range r.registry {
		result = append(result, v)
	}
	return result
}

//AddScheduled adds schedule event
func (r *Registry) AddScheduled(object storage.Object) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Scheduled[object.URL()] = object
}


//AddBatch add batch
func (r *Registry) AddBatch(object storage.Object) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Scheduled[object.URL()] = object
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
	return &Registry{registry: make(map[string]*Events), ScheduleBatches: ScheduleBatches{
		Scheduled: make(map[string]storage.Object),
		batches:   make(map[string]bool),
	}}
}
