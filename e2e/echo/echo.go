package echo

import (
	"io/ioutil"
	"net/http"
)

// Echo prints body or 200 ok
func Echo(w http.ResponseWriter, r *http.Request) {
	var data = []byte("test")
	var err error
	if r.Body != nil {
		if data, err = ioutil.ReadAll(r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = r.Body.Close()
	}
	_, err = w.Write(data)

}
