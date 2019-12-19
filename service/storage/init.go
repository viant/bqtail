package storage

import (
	"bqtail/base"
	"bqtail/task"
)

const id = "fs"

//InitRegistry initialises registry
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(base.ActionMove, task.NewServiceAction(id, MoveRequest{}))
	registry.RegisterAction(base.ActionDelete, task.NewServiceAction(id, DeleteRequest{}))
}
