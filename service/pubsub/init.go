package pubsub

import (
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

const id = "pubsub"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionPush, task.NewServiceAction(id, PushRequest{}))
}
