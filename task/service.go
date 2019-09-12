package task

import "context"

//Request represents a service request
type Request interface{}

//Service represents tasks service
type Service interface {
	Run(ctx context.Context, request Request) error
}
