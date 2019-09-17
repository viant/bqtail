package kms

import (
	"bqtail/base"
	"context"
)

//Service represents kms service
type Service interface {
	Decrypt(ctx context.Context, secret *base.Secret) ([]byte, error)
}
