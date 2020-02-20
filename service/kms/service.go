package kms

import (
	"context"
	"github.com/viant/bqtail/base"
)

//Service represents kms service
type Service interface {
	Decrypt(ctx context.Context, secret *base.Secret) ([]byte, error)
}
