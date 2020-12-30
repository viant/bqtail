package batch

import (
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

const id = "batch"

//InitRegistry initialises registry
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionGroup, task.NewServiceAction(id, GroupRequest{}))
}
