package dispatch

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/dispatch/config"
	"time"

	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"os"
	"strings"
)

//Config represents dispatchBqEvents config
type Config struct {
	base.Config
	config.Ruleset
	TimeToLiveInMin   int
	MaxConcurrentSQL  int
	MaxConcurrentLoad int
}

//TimeToLive returns time to live
func (c *Config) TimeToLive() time.Duration {
	if c.TimeToLiveInMin == 0 {
		c.TimeToLiveInMin = 1
	}
	return time.Minute*time.Duration(c.TimeToLiveInMin) - (5 * time.Second)
}

//Init initialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	err := c.Config.Init(ctx)
	if err != nil {
		return err
	}
	if c.TimeToLiveInMin == 0 {
		c.TimeToLiveInMin = 1
	}
	return c.Ruleset.Init(ctx, fs, c.ProjectID)
}

//ReloadIfNeeded reloads rules if needed
func (c *Config) ReloadIfNeeded(ctx context.Context, fs afs.Service) error {
	_, err := c.Ruleset.ReloadIfNeeded(ctx, fs)
	return err
}

//NewConfigFromEnv creates config from env
func NewConfigFromEnv(ctx context.Context, key string) (*Config, error) {
	if key == "" {
		return nil, errors.New("os env cfg key was empty")
	}
	data := os.Getenv(key)
	if data == "" {
		return nil, fmt.Errorf("env.%v was empty", key)
	}
	cfg := &Config{}
	err := json.NewDecoder(strings.NewReader(data)).Decode(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode config: %s", data)
	}
	if err = cfg.Init(ctx, afs.New()); err != nil {
		return nil, err
	}
	err = cfg.Config.Validate()
	return cfg, err
}

//NewConfigFromURL creates new config from URL
func NewConfigFromURL(ctx context.Context, URL string) (*Config, error) {
	storageService := afs.New()
	reader, err := storageService.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	err = json.NewDecoder(reader).Decode(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode config: %s", URL)
	}
	if err = cfg.Init(ctx, afs.New()); err != nil {
		return cfg, err
	}
	err = cfg.Config.Validate()
	return cfg, err
}

//NewConfig creates a new config from env (json or URL)
func NewConfig(ctx context.Context, key string) (*Config, error) {
	if key == "" {
		return nil, fmt.Errorf("config key was empty")
	}
	value := os.Getenv(key)
	if json.Valid([]byte(value)) {
		return NewConfigFromEnv(ctx, key)
	}
	return NewConfigFromURL(ctx, value)
}
