package bq

import (
	"bqtail/shared"
	"bqtail/task"
)

const id = "bq"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionCopy, task.NewServiceAction(id, CopyRequest{}))
	registry.RegisterAction(shared.ActionDrop, task.NewServiceAction(id, DropRequest{}))
	registry.RegisterAction(shared.ActionQuery, task.NewServiceAction(id, QueryRequest{}))
	registry.RegisterAction(shared.ActionPatch, task.NewServiceAction(id, PatchRequest{}))
	registry.RegisterAction(shared.ActionLoad, task.NewServiceAction(id, LoadRequest{}))
	registry.RegisterAction(shared.ActionExport, task.NewServiceAction(id, ExportRequest{}))
}
