package mon

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"time"
)

func traverse(ctx context.Context, URL string, fs afs.Service, maxCount int, stalledCount, activeCount *int, minAge time.Duration) error {
	objects, err := fs.List(ctx, URL)
	if err != nil {
		return err
	}
	for _, object := range objects {
		if url.Equals(object.URL(), URL) {
			continue
		}
		if object.IsDir() {
			if err = traverse(ctx, object.URL(), fs, maxCount, stalledCount, activeCount, minAge); err != nil {
				return err
			}
			continue
		}
		if time.Now().Sub(object.ModTime()) > minAge {
			*stalledCount++
			if *stalledCount > maxCount {
				return nil
			}
		} else {
			*activeCount++
		}
	}

	return nil
}
