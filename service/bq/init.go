package bq

import (
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

const id = "bq"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService(id, service)
	registry.RegisterAction(shared.ActionCopy, task.NewServiceAction(id, CopyRequest{}))
	registry.RegisterAction(shared.ActionDrop, task.NewServiceAction(id, DropRequest{}))
	registry.RegisterAction(shared.ActionQuery, task.NewServiceAction(id, QueryRequest{}))
	registry.RegisterAction(shared.ActionLoad, task.NewServiceAction(id, LoadRequest{}))
	registry.RegisterAction(shared.ActionExport, task.NewServiceAction(id, ExportRequest{}))
	registry.RegisterAction(shared.ActionInsert, task.NewServiceAction(id, InsertRequest{}))
	registry.RegisterAction(shared.ActionTableExists, task.NewServiceAction(id, TableExistsRequest{}))

}
