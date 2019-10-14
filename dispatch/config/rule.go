package config

import (
	"bqtail/base"
	"bqtail/task"
)

//Rule represents trigger route
type Rule struct {
	When Filter `json:",omitempty"`
	task.Actions
	Info base.Info `json:",omitempty"`
}

func (r *Rule) Init() error {
	if err := r.When.Init(); err != nil {
		return err
	}
	return nil
}
