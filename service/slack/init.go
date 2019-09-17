package slack

import "bqtail/task"

//InitRegistry initialises registry with bq actions
func InitRegistry(registry task.Registry, service Service) {
	registry.RegisterService("slack", service)
	registry.RegisterAction("notify", task.NewServiceAction("slack", NotifyRequest{}))
}
