package mon

import (
	"bqtail/tail"
	"context"
)

var singleton Service
var singletonEnvKey string

//NewFromEnv returns singleton service for env key
func Singleton(ctx context.Context, envKey string) (Service, error) {
	if singleton != nil && envKey == singletonEnvKey {
		return singleton, nil
	}
	config, err := tail.NewConfig(ctx, envKey)
	if err != nil {
		return nil, err
	}
	service, err := New(ctx, config)
	if err != nil {
		return nil, err
	}
	singletonEnvKey = envKey
	singleton = service
	return singleton, nil
}
