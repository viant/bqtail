package task

import (
	"bqtail/base"
	"bqtail/stage"
	"fmt"
	"github.com/viant/toolbox"
	"reflect"
)

//PostActioner represents PostActioner
type PostActioner interface {
	PostActions() *Actions
}

//Action represents route action
type Action struct {
	Action  string
	Request map[string]interface{}
}

//SetRequest set request for supplied req instance
func (a *Action) SetRequest(req interface{}) error {
	a.Request = map[string]interface{}{}
	err := toolbox.DefaultConverter.AssignConverted(&a.Request, req)

	return err
}

//New creates a new action
func (a Action) New(root *stage.Info, request map[string]interface{}) *Action {
	var result = &Action{
		Action:  a.Action,
		Request: make(map[string]interface{}),
	}
	for k, v := range request {
		if v == nil {
			continue
		}
		result.Request[k] = v
	}

	for k, v := range a.Request {
		if _, ok :=  result.Request[k]; ok {
			continue
		}
		result.Request[k] = v
	}
	if rootContextActions[a.Action]{
		expanded := root.ExpandMap(result.Request)
		if base.IsLoggingEnabled() {
			fmt.Printf("context map:")
			toolbox.Dump(expanded)
			fmt.Printf("expanded:")
			toolbox.Dump(expanded)
		}
		result.Request = expanded
	}

	if base.IsLoggingEnabled() {
		fmt.Printf("new action: %v\n", result.Action)
		toolbox.Dump(result)
	}

	return result
}

//NewAction creates a new action for supplied name, action
func NewAction(action string, root *stage.Info, req interface{}) (*Action, error) {
	result := &Action{Action: action}
	err := result.SetRequest(req)
	if rootContextActions[action]{
		result.Request[base.RootKey] = root.AsMap()
	}
	return result, err
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
