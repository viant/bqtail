package base

import (
	"sync"
	"time"
)

//Resources represents a resource
type Resources struct {
	mutex    *sync.RWMutex
	elements map[string]time.Time
}

//Add add url with modified time
func (r *Resources) Add(URL string, modified time.Time) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.elements[URL] = modified
}

//Remove removes URL from a elements
func (r *Resources) Remove(URL string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.elements, URL)
}

//Has returns true if has URL
func (r *Resources) Has(URL string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	_, ok := r.elements[URL]
	return ok
}

//Get returns URL last modified time
func (r *Resources) Get(URL string) *time.Time {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	modified, ok := r.elements[URL]
	if !ok {
		return nil
	}
	return &modified
}

//GetMissing returns missing in snapshot URLs
func (r *Resources) GetMissing(snapshot map[string]time.Time) []string {
	var missing = make([]string, 0)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for URL := range r.elements {
		if _, ok := snapshot[URL]; ok {
			continue
		}
		missing = append(missing, URL)
	}
	return missing
}

//NewResources creates a resources container
func NewResources() *Resources {
	return &Resources{
		mutex:    &sync.RWMutex{},
		elements: make(map[string]time.Time),
	}
}
