package http

import (
	"bqtail/base"
	"bqtail/task"
)

const id = "http"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(base.ActionCall, task.NewServiceAction(id, CallRequest{}))
}
