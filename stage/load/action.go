package load

import (
	sbatch "github.com/viant/bqtail/service/batch"
	"github.com/viant/bqtail/service/storage"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
)

func (j *Job) buildActions() (*task.Actions, error) {
	actions := j.Rule.Actions().Clone()
	if j.Window != nil {
		buildBatchActions(j.Window, actions)
		if j.Group != nil {
			j.buildGroupActions(actions)
		}
	}
	j.buildProcessActions(actions)
	result, err := j.buildTransientActions(actions)
	return result, err
}

func (j *Job) buildGroupActions(actions *task.Actions) {
	groupAction, _ := task.NewAction(shared.ActionGroup, sbatch.GroupRequest{MaxDurationInSec: j.Rule.Batch.Group.DurationInSec})
	groupAction.Actions = &task.Actions{
		OnSuccess: j.Rule.Batch.Group.OnDone,
	}
	actions.FinalizeOnSuccess(groupAction)
}

// buildDoneProcessAction append track action
func (j *Job) buildProcessActions(actions *task.Actions) {

	moveRequest := storage.MoveRequest{SourceURL: j.ProcessURL, DestURL: j.DoneProcessURL, IsDestAbsoluteURL: true}
	moveAction, _ := task.NewAction(shared.ActionMove, moveRequest)
	actions.FinalizeOnSuccess(moveAction)
}
