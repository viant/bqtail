package load

import (
	"github.com/viant/bqtail/service/storage"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

func (j *Job) buildActions() (*task.Actions, error) {
	actions := j.Rule.Actions()
	if j.Window != nil {
		buildBatchActions(j.Window, actions)
	}
	j.buildProcessActions(actions)
	result, err := j.buildTransientActions(actions)
	return result, err
}

//buildDoneProcessAction append track action
func (j *Job) buildProcessActions(actions *task.Actions) {
	moveRequest := storage.MoveRequest{SourceURL: j.ProcessURL, DestURL: j.DoneProcessURL, IsDestAbsoluteURL: true}
	moveAction, _ := task.NewAction(shared.ActionMove, moveRequest)
	actions.AddOnSuccess(moveAction)
}
