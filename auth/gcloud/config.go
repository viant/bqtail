package gcloud

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	ini "gopkg.in/ini.v1"
	"io/ioutil"
)

//Config represents gsutil config
type Config struct {
	Core *Core `ini:"Core"`
}

//ConfigFromURL creates a config from URL
func ConfigFromURL(ctx context.Context, URL string, fs afs.Service) (*Config, error) {
	object, err := fs.Object(ctx, URL)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	if object.IsDir() {
		objects, err := fs.List(ctx, object.URL())
		if err != nil {
			return nil, err
		}
		for _, obj := range objects {
			if obj.IsDir() {
				continue
			}
			if config, err = ConfigFromURL(ctx, obj.URL(), fs); config != nil && config.Core != nil {
				break
			}
		}
		return config, nil
	}

	reader, err := fs.Download(ctx, object)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read: %v", URL)
	}
	cfg, err := ini.Load(data)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load init: %v", URL)
	}
	core := cfg.Section("core")
	if core == nil {
		return nil, nil
	}
	key, err := core.GetKey("account")
	if err != nil {
		return nil, err
	}
	config.Core = &Core{Account: key.Value()}
	return config, err
}
