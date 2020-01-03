package replay

import (
	"bqtail/base"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"strings"
	"time"
)


//Service represents replay service
type Service interface {
	Replay(context.Context, *Request) *Response
}

type service struct {
	fs afs.Service
}

func (s *service) Replay(ctx context.Context, request *Request) *Response {
	response := &Response{
		Replayed: make([]string, 0),
		Status:   base.StatusOK,
	}
	err := s.replay(ctx, request, response)
	if err != nil {
		response.Status = base.StatusError
		response.Error = err.Error()
	}
	return response
}

func (s *service) replay(ctx context.Context, request *Request, response *Response) error {
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		return err
	}

	mover := newMover(s.fs)
	mover.Run(ctx, 20)

	objects, err := s.list(ctx, request.TriggerURL, request.unprocessedModifiedBefore)
	fmt.Printf("%v %v\n", request.TriggerURL, len(objects))
	for i := range objects {
		if objects[i].IsDir() {
			continue
		}
		sourceURL := objects[i].URL()
		sourceBucket := url.Host(sourceURL)
		destURL := strings.Replace(sourceURL, sourceBucket, request.ReplayBucket, 1)
		mover.Schedule(&replay{src: sourceURL, dest: destURL})
	}
	return mover.Wait()
}

func (s *service) list(ctx context.Context, URL string, modifiedBefore *time.Time) ([]storage.Object, error) {
	timeMatcher := matcher.NewModification(modifiedBefore, nil)
	recursive := option.NewRecursive(true)
	exists, _ := s.fs.Exists(ctx, URL)
	if !exists {
		return []storage.Object{}, nil
	}
	return s.fs.List(ctx, URL, timeMatcher, recursive)
}

//New creates new replay service
func New() Service {
	return &service{
		fs: afs.New(),
	}
}
