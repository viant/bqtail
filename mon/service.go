package mon

import (
	"bqtail/base"
	"bqtail/tail"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	_ "github.com/viant/afsc/gs"
	"path"
)

//Service represents monitoring service
type Service interface {
	//Check checks un process file and mirror errors
	Check(context.Context, *Request) *Response
}

type service struct {
	fs afs.Service
	*tail.Config
}


//Check checks triggerBucket and error
func (s *service) Check(ctx context.Context, request *Request) *Response {
	response := NewResponse()
	err := s.check(ctx, request, response)
	if err != nil {
		response.Error = err.Error()
		response.Status = base.StatusError
	}
	return response
}



func (s *service) check(ctx context.Context, request *Request, response *Response) (err error) {
	if err = request.Validate(); err != nil {
		return err
	}



	return nil
}



func (s *service) getScheduledBatches(ctx context.Context, URL string) (batches, error) {
	var result = make([]*batch, 0)
	objects, err := s.fs.List(ctx, URL)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.IsDir() || path.Ext(object.Name()) != base.WindowExt  {
			continue
		}
		result = append(result, parseBatch(object.Name()))
	}
	return result, nil
}









//New creates monitoring service
func New(ctx context.Context, config *tail.Config) (Service, error) {
	fs := afs.New()
	cfs := cache.New(config.URL, fs)
	err := config.Init(ctx, cfs)
	if err != nil {
		return nil, err
	}
	return &service{
		fs:     afs.New(),
		Config: config,
	}, err
}

