package bq

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) runActions(ctx context.Context, err error, parent *bigquery.Job, onDone *task.Actions) error {
	baseJob := base.Job(*parent)
	toRun := onDone.ToRun(err, &baseJob)
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
