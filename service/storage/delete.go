package storage

import (
	"context"
	"fmt"
)

func (s *service) Delete(ctx context.Context, request *DeleteRequest) error {
	request.Init()
	err := request.Validate()
	if err != nil {
		return err
	}
	for i := range request.URLs {
		if e := s.storage.Delete(ctx, request.URLs[i]); e != nil {
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

func (r *DeleteRequest) Init() {
	if len(r.SourceURL) == 0 {
		r.URLs = make([]string, 0)
	}
	if r.SourceURL != "" {
		r.URLs = append(r.URLs, r.SourceURL)
	}
}


//Validate check if request is valid
func (r *DeleteRequest) Validate() error {
	if len(r.URLs) == 0 {
		return fmt.Errorf("URLS was empty")
	}
	return nil
}
