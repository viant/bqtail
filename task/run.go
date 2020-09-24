package task

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
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

func canRun(ctx context.Context, when *When, registry Registry) (bool, error) {
	if when == nil || when.Exists == "" {
		return true, nil
	}
	exists, err := Run(ctx, registry, &Action{
		Action: shared.ActionTableExists,
		Request: map[string]interface{}{
			"table": when.Exists,
		},
	})
	return toolbox.AsBoolean(exists), err
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
	shallRun, cErr := canRun(ctx, request.When, registry)
	if cErr != nil {
		return false, cErr
	}
	if !shallRun {
		return nil, nil
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogLn(request)
	}
	var response Response
	response, err = service.Run(ctx, request)
	if shared.IsDebugLoggingLevel() && err != nil {
		shared.LogF("err: %v\n", err)
	}
	return response, err
}
