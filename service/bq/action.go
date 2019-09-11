package bq

import (
	"context"
	"fmt"
	"bqtail/base"
	"bqtail/task"
	"google.golang.org/api/bigquery/v2"
)

func (s *service) runActions(ctx context.Context, err error, parent *bigquery.Job, onDone *task.Actions) error {
	var actions []*task.Action
	if err == nil {
		actions = onDone.OnSuccess
	} else {
		actions = onDone.OnFailure
	}
	if len(actions) == 0 {
		return nil
	}
	for i := range actions {
		jobID := parent.JobReference.JobId + base.PathElementSeparator + fmt.Sprintf("%03d_%v", i, actions[i].Action)
		actions[i].SetJobID(jobID)
		if err = task.Run(ctx, s.Registry, actions[i]); err != nil {
			return err
		}
	}
	return err
}
