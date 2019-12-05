package mon

import (
	"bqtail/base"
	"github.com/pkg/errors"
)


//Request represents monitoring request
type Request struct {
	TriggerBucket             string
}



//Response represents monitoring response
type Response struct {
	Status string
	Error string
	Info
	Destinations []*Info
}


//Validate check if request is valid
func (r *Request) Validate() (err error) {
	if r.TriggerBucket == "" {
		return errors.Errorf("triggerBucket was empty")
	}
	return nil
}

//NewResponse create a response
func NewResponse() *Response {
	return &Response{
		Status: base.StatusOK,
	}
}
