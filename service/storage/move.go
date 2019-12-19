package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
)

//Move move source to destination
func (s *service) Move(ctx context.Context, request *MoveRequest) error {
	err := request.Validate()
	if err != nil {
		return err
	}

	_, sourceLocation := url.Base(request.SourceURL, file.Scheme)
	destURL := url.Join(request.DestURL, sourceLocation)

	if request.IsDestAbsoluteURL {
		destURL = request.DestURL
	}
	err = s.fs.Move(ctx, request.SourceURL, destURL)
	if err != nil {
		if exists, _ := s.fs.Exists(ctx, request.SourceURL); !exists {
			return nil
		}
	}
	return err
}

//MoveRequest represnets a move resource request
type MoveRequest struct {
	SourceURL         string
	IsDestAbsoluteURL bool
	DestURL           string
}

//Validate checks if request is valid
func (r MoveRequest) Validate() error {
	if r.DestURL == "" {
		return fmt.Errorf("destURL was empty")
	}
	if r.SourceURL == "" {
		return fmt.Errorf("sourceURL was empty")
	}
	return nil
}
