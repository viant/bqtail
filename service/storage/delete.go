package storage

import (
	"context"
	"fmt"
)

const deleteSleepTime = 300


//Delete deletes supplied URLs
func (s *service) Delete(ctx context.Context, request *DeleteRequest) error {
	request.Init()
	err := request.Validate()
	if err != nil {
		return err
	}

	deleter  := newDeleter(s.fs)
	deleter.Run(ctx, 5)

	processed := map[string]bool{}
	for i := range request.URLs {
		if processed[request.URLs[i]] {
			continue
		}
		processed[request.URLs[i]] = true
		deleter.Schedule(request.URLs[i])
	}
	return deleter.Wait()
}

//DeleteRequest delete request
type DeleteRequest struct {
	URLs      []string
	SourceURL string
}

//Init initialize request
func (r *DeleteRequest) Init() {
	if len(r.URLs) == 0 {
		r.URLs = make([]string, 0)
	}
	if r.SourceURL != "" {
		r.URLs = append(r.URLs, r.SourceURL)
	}
}

//Validate check if request is valid
func (r *DeleteRequest) Validate() error {
	if len(r.URLs) == 0 {
		return fmt.Errorf("urls was empty")
	}
	return nil
}
