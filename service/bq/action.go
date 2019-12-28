package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func (s *service) runActions(ctx context.Context, err error, parent *bigquery.Job, onDone *task.Actions) error {
	if parent == nil {
		return fmt.Errorf("parent was empty")
	}
	baseJob := base.Job(*parent)
	toRun := onDone.ToRun(err, &baseJob)
	if len(toRun) == 0 {
		return nil
	}
	if err != nil && onDone.SourceURI != "" {
		sourcePath := url.Path(onDone.SourceURI)
		errorURL := url.Join(s.Config.ErrorURL, sourcePath+base.ActionErrorExt)
		if e := s.fs.Upload(ctx, errorURL, file.DefaultFileOsMode, strings.NewReader(err.Error())); e != nil {
			return errors.Wrapf(err, "failed to write error file: %v %v", errorURL, e)
		}
	}
	_, err = task.RunAll(ctx, s.Registry, toRun)
	return err
}
