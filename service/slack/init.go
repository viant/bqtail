package slack

import (
	"bqtail/base"
	"bqtail/task"
)

const id = "slack"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(base.ActionNotify, task.NewServiceAction(id, NotifyRequest{}))
}
