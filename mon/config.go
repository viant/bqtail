package mon

import (
	"bqtail/base"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
)

//Config represents monitoring config
type Config struct {
	base.Config
}

//NewConfig create a config
func NewConfig(ctx context.Context, URL string, fs afs.Service) (*Config, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err = json.NewDecoder(reader).Decode(config); err != nil {
		return nil, errors.Wrapf(err, "failed to decode config: %v", URL)
	}
	return config, nil
}
