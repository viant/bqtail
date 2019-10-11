package storage

import "bqtail/task"

//InitRegistry initialises registry
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService("fs", service)
	registry.RegisterAction("move", task.NewServiceAction("fs", MoveRequest{}))
	registry.RegisterAction("delete", task.NewServiceAction("fs", DeleteRequest{}))
}
