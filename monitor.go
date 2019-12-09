package bqtail

import (
	"net/http"
)

//BqTailMonitor cloud function entry point
func BqTailMonitor(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > 0 {
		defer func() {
			_ = r.Body.Close()
		}()
	}
	//work in progress...
}
