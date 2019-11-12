package bqtail

import (
	"bqtail/dispatch"
	"bqtail/dispatch/replay"
	"context"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"net/http"
)


//BqDispatch BigQuery trigger background cloud function entry point
func BqDispatchReplay(w http.ResponseWriter, r *http.Request) (err error) {
	ctx := context.Background()
	srv, err := dispatch.Singleton(ctx)
	if err != nil {
		return  err
	}
	replayer := replay.New(srv, afs.New())
	repsponse := replayer.Replay(ctx)
	toolbox.Dump(repsponse)
	return nil
}

