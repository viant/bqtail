package storage

import (
	"bqtail/shared"
	"bqtail/task"
)

const id = "fs"

//InitRegistry initialises registry
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionMove, task.NewServiceAction(id, MoveRequest{}))
	registry.RegisterAction(shared.ActionDelete, task.NewServiceAction(id, DeleteRequest{}))
}
