package replay

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
)


//Response represents a response
type Response struct {
	base.Response
	Processed []*contract.Response

}


func (r *Response) AddResponse(response *contract.Response) {
	r.Processed = append(r.Processed, response)
}

//NewResponse create a new request
func NewResponse() *Response{
	return &Response{
		Response: base.Response{
			Status:base.StatusOK,
		},
		Processed: make([] *contract.Response, 0),
	}
}
