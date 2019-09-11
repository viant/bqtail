package bq

import "bqtail/task"


func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService("bq", service)
	registry.RegisterAction("copy", task.NewServiceAction("bq", CopyRequest{}))
	registry.RegisterAction("query", task.NewServiceAction("bq", QueryRequest{}))
	registry.RegisterAction("load", task.NewServiceAction("bq", LoadRequest{}))
	registry.RegisterAction("export", task.NewServiceAction("bq", ExportRequest{}))
}
