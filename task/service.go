package task

import "context"

//Request represents a service request
type Request interface{}

type Response interface{}

//Service represents tasks service
type Service interface {
	Run(ctx context.Context, request Request) (Response, error)
}
