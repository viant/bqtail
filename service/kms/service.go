package kms

import (
	"bqtail/base"
	"context"
)

type Service interface {
	Decrypt(ctx context.Context, secret *base.Secret) ([]byte, error)
}
