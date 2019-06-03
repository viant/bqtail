package bqtail

import (
	"bqtail/model"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/toolbox/url"
	"os"
	"strings"
)

type Config struct {
	Credentials string
	Rules       *model.Rules
	//RunOnce each invocation creates a new service instance (test mode)
	RunOnce bool
}

func (c *Config) Init() {
	if c.Rules == nil {
		return
	}
	c.Rules.Init()
}

func (c *Config) Validate() error {
	if c.Rules == nil {
		return errors.New("rules were empty")
	}

	return c.Rules.Validate()
}

//NewConfigFromEnv creates config from env
func NewConfigFromEnv(key string) (*Config, error) {
	if key == "" {
		return nil, errors.New("os env config key was empty")
	}
	data := os.Getenv(key)
	if data == "" {
		return nil, fmt.Errorf("env.%v was empty", key)
	}
	config := &Config{}
	err := json.NewDecoder(strings.NewReader(data)).Decode(config)
	if err == nil {
		config.Init()
		err = config.Validate()
	}
	return config, err
}

//NewConfigFromURL creates new config from URL
func NewConfigFromURL(URL string) (*Config, error) {
	resource := url.NewResource(URL)
	config := &Config{}
	err := resource.Decode(config)
	if err == nil {
		config.Init()
		err = config.Validate()
	}
	return config, err
}

//NewConfig creates a new config from env (json or URL)
func NewConfig(key string) (*Config, error) {
	if key == "" {
		return nil, fmt.Errorf("config key was empty")
	}
	value := os.Getenv(key)
	if json.Valid([]byte(value)) {
		return NewConfigFromEnv(key)
	}
	return NewConfigFromURL(value)
}
