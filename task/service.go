package task

import (
	"context"
	"github.com/viant/toolbox"
	"reflect"
)

type Response interface{}


//Service represents tasks service
type Service interface {
	Run(ctx context.Context, request *Action) (Response, error)
}


//ServiceAction represets service action
type ServiceAction struct {
	Service     string
	RequestType reflect.Type
}

//NewServiceAction creates a service action
func NewServiceAction(service string, request interface{}) *ServiceAction {
	return &ServiceAction{
		Service:     service,
		RequestType: reflect.TypeOf(request),
	}
}



//SetServiceRequest creates a new request
func (a *ServiceAction) SetServiceRequest(action *Action) error {
	result := reflect.New(a.RequestType).Interface()
	err := toolbox.DefaultConverter.AssignConverted(result, action.Request)
	if err != nil {
		return err
	}
	err = action.SetRequest(result)
	return err
}
