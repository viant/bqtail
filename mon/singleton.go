package mon

import (
	"context"
)

var singleton Service
var singletonEnvKey string

//NewFromEnv returns singleton service for env key
func NewFromEnv(ctx context.Context, envKey string) (Service, error) {
	if singleton != nil && envKey == singletonEnvKey {
		return singleton, nil
	}
	config := &Config{}
	service, err := New(ctx, config)
	if err != nil {
		return nil, err
	}
	singletonEnvKey = envKey
	singleton = service
	return singleton, nil
}
