package storage

import (
	"context"
	"fmt"
)

//Delete deletes supplied URLs
func (s *service) Delete(ctx context.Context, request *DeleteRequest) error {
	request.Init()
	err := request.Validate()
	if err != nil {
		return err
	}
	processed := map[string]bool{}
	for i := range request.URLs {
		if processed[request.URLs[i]] {
			continue
		}
		processed[request.URLs[i]] = true
		if e := s.fs.Delete(ctx, request.URLs[i]); e != nil {
			if ok, _ := s.fs.Exists(ctx, request.URLs[i]); !ok {
				continue
			}
			err = e
		}
	}
	return err
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
