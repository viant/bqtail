package bq

import (
	"bqtail/base"
	"bqtail/task"
)

const id = "bq"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(base.ActionCopy, task.NewServiceAction(id, CopyRequest{}))
	registry.RegisterAction(base.ActionDrop, task.NewServiceAction(id, DropRequest{}))
	registry.RegisterAction(base.ActionQuery, task.NewServiceAction(id, QueryRequest{}))
	registry.RegisterAction(base.ActionPatch, task.NewServiceAction(id, PatchRequest{}))
	registry.RegisterAction(base.ActionLoad, task.NewServiceAction(id, LoadRequest{}))
	registry.RegisterAction(base.ActionExport, task.NewServiceAction(id, ExportRequest{}))
}
