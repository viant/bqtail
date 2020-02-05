package http

import (
	"bqtail/shared"
	"bqtail/task"
)

const id = "http"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionCall, task.NewServiceAction(id, CallRequest{}))
}
