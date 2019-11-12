package bqtail

import (
	"bqtail/base"
	"bqtail/dispatch"
	"bqtail/dispatch/replay"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
)


//BqDispatch BigQuery trigger background cloud function entry point
func BqDispatchReplay(ctx context.Context, event interface{}) (err error) {
	if base.IsLoggingEnabled() {
		fmt.Printf("startning dispatch replay: %v\n", event)
	}

	srv, err := dispatch.Singleton(ctx)
	if err != nil {
		return  err
	}
	replayer := replay.New(srv, afs.New())
	repsponse := replayer.Replay(ctx)
	toolbox.Dump(repsponse)
	return nil
}

