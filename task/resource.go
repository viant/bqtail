package task

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"io/ioutil"
)

//load loads external resource
func load(ctx context.Context, fs afs.Service, URL string) (string, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return "", errors.Wrapf(err, "failed to load data from: %v", URL)
	}
	payload, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read data from: %v", URL)
	}
	return string(payload), nil
}
