package batch

import (
	"context"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/task"
	"time"
)

//GroupRequest represents gr
type GroupRequest struct {
	MaxDurationInSec int
}

func (s *service) OnDone(ctx context.Context, req *GroupRequest, action *task.Action) error {
	group := batch.NewGroup(action.Meta.GroupURL, s.fs)
	count, err := group.Decrement(ctx)
	if err != nil {
		return err
	}
	isGroupDone := count == 0
	if req.MaxDurationInSec > 0 && !isGroupDone {
		//check age for stalled group (sanity check, in case group never end)
		if object, _ := s.fs.Object(ctx, action.Meta.GroupURL); object != nil {
			isGroupDone = time.Now().Sub(object.ModTime()) > time.Duration(req.MaxDurationInSec)*time.Second
		}
	}
	if isGroupDone {
		_, err = task.RunAll(ctx, s.registry, action.OnSuccess)
		group.Delete(ctx)
	}
	if shared.IsInfoLoggingLevel() {
		shared.LogF("[%v] checking group:%v, count:%v, done:%v, URL:%v\n", action.Meta.DestTable, group.ID(), count, isGroupDone, group.URL)
	}
	return err
}
