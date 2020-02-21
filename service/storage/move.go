package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/shared"
)

//Move move source to destination
func (s *service) Move(ctx context.Context, request *MoveRequest) error {
	err := request.Validate()
	if err != nil {
		return err
	}

	if request.SourceURL != "" {
		if moveErr := s.move(ctx, request.IsDestAbsoluteURL, request.SourceURL, request.DestURL); moveErr != nil {
			err = moveErr
		}
	}
	if len(request.SourceURLs) > 0 {
		for _, sourceURL := range request.SourceURLs {
			if moveErr := s.move(ctx, request.IsDestAbsoluteURL, sourceURL, request.DestURL); moveErr != nil {
				err = moveErr
			}
		}
	}
	return err
}

func (s *service) move(ctx context.Context, isDestAbsoluteURL bool, sourceURL, destURL string) error {
	_, sourceLocation := url.Base(sourceURL, file.Scheme)
	if !isDestAbsoluteURL {
		destURL = url.Join(destURL, sourceLocation)
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogLn(fmt.Sprintf("moving: %v -> %v\n", sourceURL, destURL))
	}

	err := s.fs.Move(ctx, sourceURL, destURL)
	if err != nil {
		if exists, existErr := s.fs.Exists(ctx, sourceURL, option.NewObjectKind(true)); !exists && existErr == nil {
			err = nil
		}
	}
	return err
}

//MoveRequest represnets a move resource request
type MoveRequest struct {
	SourceURL         string
	SourceURLs        []string
	IsDestAbsoluteURL bool
	DestURL           string
}

//Validate checks if request is valid
func (r MoveRequest) Validate() error {
	if r.DestURL == "" {
		return fmt.Errorf("destURL was empty")
	}
	if r.SourceURL == "" && len(r.SourceURLs) == 0 {
		return fmt.Errorf("sourceURL was empty")
	}
	return nil
}
