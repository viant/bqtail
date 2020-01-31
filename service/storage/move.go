package storage

import (
	"bqtail/base"
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
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
	if base.IsLoggingEnabled() {
		base.Log(fmt.Sprintf("moving: %v -> %v\n", request.SourceURL, destURL))
	}
	err = s.fs.Move(ctx, request.SourceURL, destURL)
	if err != nil {
		if exists, err := s.fs.Exists(ctx, request.SourceURL, option.NewObjectKind(true)); !exists && err == nil {
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
