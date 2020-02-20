package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
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
	_, err = task.RunAll(ctx, s.Registry, toRun)
	return err
}
