package task

import (
	"github.com/viant/toolbox"
	"reflect"
	"strings"
)

//Action represents route action
type Action struct {
	Action  string
	Request map[string]interface{}
}

//SetRequest set request for supplied req instance
func (a *Action) SetRequest(req interface{}) error {
	a.Request = map[string]interface{}{}

	return toolbox.DefaultConverter.AssignConverted(&a.Request, req)
}

//New creates a new action
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
	result := &Action{Action: action}
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
	var req = map[string]interface{}{}

	for k, v := range action.Request {
		req[k] = v
		text, ok := v.(string)
		if !ok {
			continue
		}
		for key, exp := range replacements {
			value, ok := action.Request[key]
			if !ok {
				continue
			}
			exprValue := value.(string)
			if count := strings.Count(text, exp); count > 0 {
				text = strings.Replace(text, exp, exprValue, count)
			}
		}
		req[k] = text
	}
	err := toolbox.DefaultConverter.AssignConverted(result, req)
	return result, err
}

//NewServiceAction creates a service action
func NewServiceAction(service string, request interface{}) *ServiceAction {
	return &ServiceAction{
		Service:     service,
		RequestType: reflect.TypeOf(request),
	}
}
