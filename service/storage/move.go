package storage

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/shared"
)

const moveRoutines = 8

//Move move source to destination
func (s *service) Move(ctx context.Context, request *MoveRequest) error {
	err := request.Validate()
	if err != nil {
		return err
	}

	mover := newMover(s.fs)
	mover.Run(ctx, moveRoutines)

	if request.SourceURL != "" {
		s.move(ctx, mover, request.IsDestAbsoluteURL, request.SourceURL, request.DestURL)
	}
	if len(request.SourceURLs) > 0 {
		for _, sourceURL := range request.SourceURLs {
			s.move(ctx, mover, request.IsDestAbsoluteURL, sourceURL, request.DestURL)
		}
	}
	return mover.Wait()
}

func (s *service) move(ctx context.Context, mover *mover, isDestAbsoluteURL bool, sourceURL, destURL string) {
	_, sourceLocation := url.Base(sourceURL, file.Scheme)
	if !isDestAbsoluteURL {
		destURL = url.Join(destURL, sourceLocation)
	}
	if shared.IsDebugLoggingLevel() {
		shared.LogLn(fmt.Sprintf("moving: %v -> %v\n", sourceURL, destURL))
	}
	mover.Schedule(&move{src: sourceURL, dest: destURL})
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
