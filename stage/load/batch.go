package load

import (
	"github.com/viant/bqtail/service/storage"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/task"
	"strings"
)

//buildBatchActions add batch clean up action - to remove all batch meta data files.
func buildBatchActions(window *batch.Window, actions *task.Actions) {
	if window == nil {
		return
	}
	URLsToDelete := make([]string, 0)
	if ! window.Async { //in async mode, dispatcher removed batched file, once scheduled
		URLsToDelete = append(URLsToDelete, window.URL)
		URLsToDelete = append(URLsToDelete, strings.Replace(window.URL, shared.WindowExt, shared.WindowExtScheduled, 1))
	}
	if len(window.Locations) > 0 {
		URLsToDelete = append(URLsToDelete, window.Locations...)
	}
	if len(URLsToDelete) == 0 {
		return
	}
	deleteReq := storage.DeleteRequest{URLs: URLsToDelete}
	deleteAction, _ := task.NewAction(shared.ActionDelete, deleteReq)
	actions.AddOnSuccess(deleteAction)
}
