package storage

import (
	"context"
	"fmt"
)

func (s *service) Move(ctx context.Context, request *MoveRequest) error {
	err := request.Validate()
	if err != nil {
		return err
	}
	return s.storage.Move(ctx, request.SourceURL, request.DestURL)
}

type MoveRequest struct {
	SourceURL string
	DestURL string
}

func (r MoveRequest) Validate() error {
	if r.DestURL == "" {
		return fmt.Errorf("destURL was empty")
	}
	if r.SourceURL == "" {
		return fmt.Errorf("sourceURL was empty")
	}
	return nil
}

