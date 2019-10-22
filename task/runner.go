package task

import (
	"bqtail/base"
	"context"
	"fmt"
	"github.com/viant/toolbox"
)

//Run execute supplied actions
func Run(ctx context.Context, registry Registry, action *Action) error {
	serviceAction, err := registry.Action(action.Action)
	if err != nil {
		return err
	}
	request, err := serviceAction.NewRequest(action)
	if err != nil {
		return err
	}
	return RunWithService(ctx, registry, serviceAction.Service, request)
}

//RunWithService handlers service request or error
func RunWithService(ctx context.Context, registry Registry, serviceName string, request Request) error {
	service, err := registry.Service(serviceName)
	if err != nil {
		return err
	}
	if base.IsLoggingEnabled() {
		fmt.Printf("running %T\n", service)
		toolbox.Dump(request)
	}
	err = service.Run(ctx, request)
	if base.IsLoggingEnabled() && err != nil {
		fmt.Printf("err: %v\n", err)
	}
	return err
}
