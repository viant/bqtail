package task

import (
	"bqtail/shared"
)

var sourceURLExpandable = map[string]bool{
	shared.ActionMove:   true,
	shared.ActionDelete: true,
}

//bodyAppendable job appendable task
var bodyAppendable = map[string]bool{
	shared.ActionNotify: true,
}

//rootContextActions represents nop actions
var rootContextActions = map[string]bool{
	shared.ActionQuery:  true,
	shared.ActionNotify: true,
	shared.ActionCall:   true,
}
