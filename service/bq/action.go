package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) runActions(ctx context.Context, err error, parent *bigquery.Job, onDone *task.Actions) error {
	if parent == nil {
		return fmt.Errorf("parent was empty")
	}
	baseJob := base.Job(*parent)

	toRun := onDone.ToRun(err, &baseJob, s.deferTaskURL)
	if len(toRun) == 0 {
		return nil
	}
	for i := range toRun {
		if err = task.Run(ctx, s.Registry, toRun[i]); err != nil {
			return err
		}
	}
	return err
}
