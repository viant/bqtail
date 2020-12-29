package auth

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
)

func ClientFromURL(URL string) (*Client, error) {
	fs := afs.New()
	data, err := fs.DownloadWithURL(context.Background(), URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get client secret: %v", URL)
	}
	result := &Client{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode client secret: %v", URL)
	}
	return result, nil
}
