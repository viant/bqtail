package load

import (
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage/activity"
	"github.com/viant/bqtail/task"
)

//NewLoadRequest create a load request
func (j *Job) NewLoadRequest() (*bq.LoadRequest, *task.Action) {
	load := *j.Load
	root := j.Process
	loadRequest := &bq.LoadRequest{
		Append:               j.Rule.IsAppend(),
		JobConfigurationLoad: &load,
	}
	meta := activity.New(root, shared.ActionLoad, root.Mode(shared.ActionLoad), root.IncStepCount())
	actions := j.Actions.Expand(root, shared.ActionLoad, load.SourceUris)
	action := &task.Action{
		Action:  shared.ActionLoad,
		Actions: actions,
		Meta:    meta,
	}
	_ = action.SetRequest(loadRequest)
	return loadRequest, action
}
