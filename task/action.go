package task

import (
	"bqtail/base"
	"github.com/viant/toolbox"
	"reflect"
)

//Action represents route action
type Action struct {
	Action  string
	Request map[string]interface{}
}

func (a *Action) SetJobID(jobID string) {
	a.Request[base.JobIDKey] = jobID
}

//SetRequest set reqeust for supplied req instance
func (a *Action) SetRequest(req interface{}) error {
	a.Request = map[string]interface{}{}
	return toolbox.DefaultConverter.AssignConverted(&a.Request, req)
}

func (a Action) New(request map[string]interface{}) *Action {
	var result = &Action{
		Action:  a.Action,
		Request: make(map[string]interface{}),
	}
	for k, v := range request {
		result.Request[k] = v
	}
	for k, v := range a.Request {
		result.Request[k] = v
	}
	return result
}



//NewAction creates a new action for supplied name, action
func NewAction(action string, req interface{}) (*Action, error) {
	result := &Action{Action:action}
	return result, result.SetRequest(req)
}



//ServiceAction represets service action
type ServiceAction struct {
	Service     string
	RequestType reflect.Type
}

//NewRequest creates a new request
func (a *ServiceAction) NewRequest(action *Action) (Request, error) {
	result := reflect.New(a.RequestType).Interface()
	err := toolbox.DefaultConverter.AssignConverted(result, action.Request)
	return result, err
}

//NewServiceAction creates a service action
func NewServiceAction(service string, request interface{}) *ServiceAction {
	return &ServiceAction{
		Service:     service,
		RequestType: reflect.TypeOf(request),
	}
}
