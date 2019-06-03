package bqtail

import (
	_ "github.com/viant/toolbox/storage/gs"
)

const ConfigKey = "CONFIG"

var srv Service

func GetService() (Service, error) {
	if srv != nil {
		return srv, nil
	}
	config, err := NewConfig(ConfigKey)
	if err != nil {
		return nil, err
	}

	if config.RunOnce {
		return New(config), nil
	}
	srv = New(config)
	return srv, err
}
