package load

import (
	"github.com/viant/bqtail/service/storage"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/task"
)

//buildBatchActions add batch clean up action - to remove all batch meta data files.
func buildBatchActions(window *batch.Window, actions *task.Actions) {
	if window == nil {
		return
	}
	URLsToDelete := make([]string, 0)
	URLsToDelete = append(URLsToDelete, window.URL)
	if len(window.Locations) > 0 {
		URLsToDelete = append(URLsToDelete, window.Locations...)
	}
	deleteReq := storage.DeleteRequest{URLs: URLsToDelete}
	deleteAction, _ := task.NewAction(shared.ActionDelete, deleteReq)
	actions.AddOnSuccess(deleteAction)
}
