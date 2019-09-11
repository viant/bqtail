package config

import "bqtail/task"

//Route represents trigger route
type Route struct {
	When    Filter
	task.Actions
}



