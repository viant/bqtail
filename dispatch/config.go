package dispatch

import (
	"bqtail/base"
	"bqtail/dispatch/config"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/storage"
	"os"
	"strings"
)

//Config represents dispatch config
type Config struct {
	base.Config
	Routes config.Routes
	RoutesBaseURL string
}

func (c *Config) loadRoutes(ctx context.Context) error {
	if c.RoutesBaseURL == "" {
		return nil
	}
	fs := afs.New()
	routesObject, err := fs.List(ctx, c.RoutesBaseURL, matcher.NewBasic("", ".json", ""))
	if err != nil {
		return err
	}
	for _, object := range routesObject {
		if err = c.loadRoute(ctx, fs, object);err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) loadRoute(ctx context.Context, storage afs.Service, object storage.Object) error {
	reader, err := storage.Download(ctx, object)
	if err != nil {
		return err
	}
	defer func() {
		_ = reader.Close()
	}()
	routes := config.Routes{}
	if err = json.NewDecoder(reader).Decode(&routes);err == nil {
		c.Routes = append(c.Routes, routes...)
	}
	return err
}


//Init initialises config
func (c *Config) Init(ctx context.Context) error {
	if len(c.Routes) == 0 {
		c.Routes = make(config.Routes, 0)
	}
	if err := c.loadRoutes(ctx);err != nil {
		return err
	}

	for i := range c.Routes {
		if err := c.Routes[i].When.Init(); err != nil {
			return errors.Wrap(err, "failed to initialise rotues")
		}
	}
	return c.Config.Init(ctx)
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
	if err == nil {
		if err = cfg.Init(ctx); err != nil {
			return nil, err
		}
		err = cfg.Validate()
	}
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
	if err == nil {
		if err = cfg.Init(ctx); err != nil {
			return cfg, err
		}
		err = cfg.Validate()
	}
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
