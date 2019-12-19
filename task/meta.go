package task

import "bqtail/base"

var sourceURLExpandable = map[string]bool{
	base.ActionMove:   true,
	base.ActionDelete: true,
}

//bodyAppendable job appendable task
var bodyAppendable = map[string]bool{
	base.ActionNotify: true,
}

//rootContextActions represents nop actions
var rootContextActions = map[string]bool{
	base.ActionQuery:  true,
	base.ActionNotify: true,
}
