package replay

import (
	"bqtail/base"
	"bqtail/dispatch"
	"bqtail/dispatch/contract"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"time"
)

var gracePeriod = time.Minute

type Service interface {
	Replay(ctx context.Context) *Response
}

type service struct {
	dispatch.Service
	fs afs.Service
}

func (s *service) Replay(ctx context.Context) *Response {
	response := NewResponse()
	err:= s.replay(ctx, response)
	if err != nil {
		response.Error = err.Error()
		response.Status = base.StatusError
	}
	return response
}



func (s *service) shallRun(ctx context.Context, jobID string) (*bigquery.Job, error) {

	if base.IsLoggingEnabled() {
		fmt.Printf("Project: %v, job: %v\n", s.Config().ProjectID, jobID)
	}

	job, err := s.BQService().GetJob(ctx, s.Config().ProjectID, jobID)
	if err != nil {
		return nil, err
	}
	if base.IsLoggingEnabled() {
		toolbox.Dump(job)
	}
	if job.Status == nil || job.Status.State != base.DoneStatus {
		return nil, nil
	}
	unixTimestamp := job.Statistics.EndTime / 1000
	endTime := time.Unix(unixTimestamp, 0)
	if time.Now().Sub(endTime) < gracePeriod {
		return nil, nil
	}
	return job, nil
}

func (s *service) replay(ctx context.Context, response *Response) error {
	jobMatcher, _ := matcher.NewBasic("", fmt.Sprintf("%v.json", base.DispatchJob), "", nil)
	modifiedBefore := time.Now().Add(-gracePeriod)
	timeMatcher := matcher.NewModification(&modifiedBefore, nil, jobMatcher.Match)
	candidate, err := s.fs.List(ctx, s.Config().DeferTaskURL, option.NewRecursive(true), timeMatcher)
	if err != nil {
		return errors.Wrapf(err, "failed to list jobs %v", s.Config().DeferTaskURL)
	}
	for i := range candidate {
		if candidate[i].IsDir() {
			continue
		}
		jobID := JobID(s.Config().DeferTaskURL, candidate[i].URL())
		if jobID == "" {
			continue
		}
		job, err := s.shallRun(ctx, jobID)
		if err != nil || job == nil {
			continue
		}
		if base.IsLoggingEnabled() {
			fmt.Printf("checking %v %v\n", s.Config().DeferTaskURL, candidate[i].URL())
		}
			baseJob := base.Job(*job)
		resp := s.Service.Dispatch(ctx, &contract.Request{
			EventID:   fmt.Sprintf("eid%04d", i) + job.JobReference.JobId,
			JobID:     job.JobReference.JobId,
			ProjectID: s.Config().ProjectID,
			Job:       &baseJob,
		})
		response.AddResponse(resp)
	}
	return nil
}

//New creates a new service
func New(srv  dispatch.Service, fs afs.Service) Service {
	return &service{
		fs:fs,
		Service:srv,
	}
}