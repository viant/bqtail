package slack

import (
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

const id = "slack"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionNotify, task.NewServiceAction(id, NotifyRequest{}))
}
