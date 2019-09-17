package secret

import (
	"bqtail/base"
	"bqtail/service/kms"
	"bqtail/service/kms/gcp"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
)

//Service represents secrets service to decode encrypted sensitive app data
type Service interface {
	Decode(ctx context.Context, service afs.Service, secret *base.Secret, target interface{}) error
}

type service struct{}

//Kms returns kms service
func (s service) Kms(service afs.Service) (kms.Service, error) {
	//TODO add other KMS vernor
	return gcp.New(service), nil
}

//Init initialises resources
func (s *service) Decode(ctx context.Context, service afs.Service, secret *base.Secret, target interface{}) error {
	kmsService, err := s.Kms(service)
	if err != nil {
		return err
	}
	data, err := kmsService.Decrypt(ctx, secret)
	if err != nil {
		return errors.Wrapf(err, "fail to decrypt %v, with %v", secret.URL, secret.Key)
	}
	data = decodeBase64IfNeeded(data)
	switch val := target.(type) {
	case *string:
		*val = string(data)
	default:
		err = json.Unmarshal(data, target)
		if err != nil {
			return errors.Wrapf(err, "unable to unmarshal: %s", data)
		}
	}
	return nil
}

//New creates a new secret service
func New() Service {
	return &service{}
}
