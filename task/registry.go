package task

import (
	"fmt"
	"sync"
)

//Registry represents services actions
type Registry interface {
	Service(name string) (Service, error)

	RegisterService(name string, service Service)

	RegisterAction(name string, service *ServiceAction)

	Action(name string) (*ServiceAction, error)

	Actions(service string) []string
}

type registry struct {
	services map[string]Service
	actions  map[string]*ServiceAction
	mutex    *sync.RWMutex
}

//Register register service
func (r *registry) RegisterService(name string, service Service) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.services[name] = service
}

//Rule returns a service
func (r *registry) Service(name string) (Service, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	service, ok := r.services[name]
	if ok {
		return service, nil
	}
	return nil, fmt.Errorf("failed to lookup service: %v", name)
}

func (r *registry) RegisterAction(name string, action *ServiceAction) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.actions[name] = action
}

func (r registry) Actions(service string) []string {
	var result = make([]string, 0)
	for action, serviceAction := range r.actions {
		if serviceAction.Service == service {
			result = append(result, action)
		}
	}
	return result
}

func (r *registry) Action(name string) (*ServiceAction, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	action, ok := r.actions[name]
	if ok {
		return action, nil
	}
	return nil, fmt.Errorf("failed to lookup action: %v", name)
}

func newRegistry() Registry {
	return &registry{
		services: make(map[string]Service),
		actions:  make(map[string]*ServiceAction),
		mutex:    &sync.RWMutex{},
	}
}

//NewRegistry returns a service action registry
func NewRegistry() Registry {
	return newRegistry()
}
