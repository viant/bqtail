package tail

import (
	"github.com/viant/bqtail/base"
	disp "github.com/viant/bqtail/dispatch/contract"
	"github.com/viant/bqtail/shared"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
)

//LoadProjectPerformance loads project performance
func LoadProjectPerformance(ctx context.Context, fs afs.Service, config *base.Config) (disp.ProjectPerformance, error) {
	URL := url.Join(config.JournalURL, shared.PerformanceFile)
	if ok, _ := fs.Exists(ctx, URL); !ok {
		return nil, nil
	}
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	result := disp.ProjectPerformance{}
	err = json.NewDecoder(reader).Decode(&result)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode performance")
	}
	return result, err
}
