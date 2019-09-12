package dispatch

import (
	"context"
	_ "github.com/viant/afsc/gs"
)

const ConfigKey = "CONFIG"

var srv Service

//Singleton returns service
func Singleton(ctx context.Context) (Service, error) {
	if srv != nil {
		return srv, nil
	}
	config, err := NewConfig(ctx, ConfigKey)
	if err != nil {
		return nil, err
	}
	if config.RunOnce {
		return New(ctx, config)
	}
	srv, err = New(ctx, config)
	return srv, err
}
