package replay

import (
	"bqtail/base"
	"bqtail/dispatch/contract"
)

//Response represents a response
type Response struct {
	base.Response
	Jobs []*contract.Response
	Processed []string
	Errored []string
}

func (r *Response) AddResponse(response *contract.Response) {
	r.Jobs  = append(r.Jobs, response)
	if response.Error != "" {
		r.Errored = append(r.Errored, response.EventID)
		return
	}
	r.Processed = append(r.Processed, response.EventID)

}

//NewResponse create a new request
func NewResponse() *Response {
	return &Response{
		Response: base.Response{
			Status: base.StatusOK,
		},
		Jobs: make([]*contract.Response, 0),
		Processed: make([]string, 0),
		Errored: make([]string, 0),
	}
}
