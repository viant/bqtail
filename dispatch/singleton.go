package dispatch

import (
	"bqtail/shared"
	"context"
	//load google fs connector
	_ "github.com/viant/afsc/gs"
)

var srv Service

//Singleton returns service
func Singleton(ctx context.Context) (Service, error) {
	if srv != nil {
		return srv, nil
	}
	config, err := NewConfig(ctx, shared.ConfigEnvKey)
	if err != nil {
		return nil, err
	}
	if config.RunOnce {
		return New(ctx, config)
	}
	srv, err = New(ctx, config)
	return srv, err
}
