package cache

import "bqtail/base"

//Response
type Response struct {
	*base.Response
	CacheURL string
	Objects  []string
}

//NewResponse creates a new response
func NewResponse() *Response {
	return &Response{
		Response: base.NewResponse(""),
	}
}
