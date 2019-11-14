package bqtail

import (
	"bqtail/dispatch"
	"bqtail/dispatch/replay"
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"net/http"
)

//BqDispatch BigQuery trigger background cloud function entry point
func BqDispatchReplay(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	srv, err := dispatch.Singleton(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	replayer := replay.New(srv, afs.New())
	response := replayer.Replay(ctx)
	toolbox.Dump(response)
	_ = json.NewEncoder(w).Encode(response)
}
