package endpoint

import (
	"net/http"
	"net/url"
)

var content = `
<!DOCTYPE html>
<html><head><title>Auth Callback</title></head><body onload="window.close();"></body></html>
`

type httpHandler struct {
	values url.Values
	done   chan bool
}

func (h *httpHandler) handle(writer http.ResponseWriter, request *http.Request) {
	if err := request.ParseForm(); err == nil {
		h.values = request.Form
	}
	writer.Header().Set("Content-Type", "text/html")
	writer.Write([]byte(content))
	h.done <- true
}

//ServeHTTP handler server requests
func (h *httpHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.handle(writer, request)
}

//AuthCode returns auth codde
func (h *httpHandler) AuthCode() string {
	if len(h.values) > 0 {
		return h.values.Get("code")
	}
	return ""
}

func newHttpHandler() *httpHandler {
	return &httpHandler{
		done: make(chan bool, 2),
	}
}
