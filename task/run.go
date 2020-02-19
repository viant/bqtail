package task

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
)

//RunAll runs all actions
func RunAll(ctx context.Context, registry Registry, actions []*Action) (bool, error) {
	if len(actions) == 0 {
		return false, nil
	}
	var err error
	var taskResponse Response
	retriable := false
	for i := range actions {
		if i > 0 && taskResponse != nil {
			aMap := map[string]interface{}{}
			err = toolbox.DefaultConverter.AssignConverted(&aMap, taskResponse)
			expandable := data.Map(aMap)
			runRequest := expandable.Expand(actions[i].Request)
			actions[i].Request = toolbox.AsMap(runRequest)
		}
		if taskResponse, err = Run(ctx, registry, actions[i]); err != nil {
			if !retriable {
				retriable = base.IsRetryError(err)
			}
			return retriable, err
		}
	}
	return retriable, nil
}

//Run execute supplied actions
func Run(ctx context.Context, registry Registry, action *Action) (Response, error) {
	serviceAction, err := registry.Action(action.Action)
	if err != nil {
		return nil, err
	}
	err = serviceAction.SetServiceRequest(action)
	if err != nil {
		return nil, err
	}
	resp, err := RunWithService(ctx, registry, serviceAction.Service, action)
	if err != nil {
		err = errors.Wrapf(err, "failed to run %v.%v", serviceAction.Service, action.Action)
	}
	return resp, err
}


//RunWithService handlers service request or error
func RunWithService(ctx context.Context, registry Registry, serviceName string, request *Action) (Response, error) {
	service, err := registry.Service(serviceName)
	if err != nil {
		return nil, err
	}
	if base.IsLoggingEnabled() {
		base.Log(request)
	}
	var response Response
	response, err = service.Run(ctx, request)
	if base.IsLoggingEnabled() && err != nil {
		fmt.Printf("err: %v\n", err)
	}
	return response, err
}
