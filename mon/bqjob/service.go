package colector

import (
	"bqtail/base"
	"bqtail/service/bq"
	"bqtail/stage"
	"bqtail/task"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	_ "github.com/viant/afsc/gs"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)


type Service interface {
	Collect(ctx context.Context, request *Request) *Response
}


type service struct {

}


func (s *service) Collect(ctx context.Context, request *Request) *Response {
	response := &Response{}
	err := s.collect(ctx, request, response)
	if err != nil {
		response.Error = err.Error()
		response.Status = base.StatusError
	}
	return response
}



func (s *service) collect(ctx context.Context, request *Request, response *Response) error {
	bqService, err := bigquery.NewService(ctx)
	if err != nil {
		return err
	}
	bq := bq.New(bqService, task.NewRegistry(), request.ProjectID, afs.New(), base.Config{})

	list, err := bq.ListJob(ctx, request.ProjectID, request.MinTime, request.MaxTime)

	fmt.Printf("list %v\n", len(list))
	if err != nil {
		return err
	}



	var infoList = make([]string, 0)

	for _, job := range list {
		startTime := time.Unix(job.Statistics.StartTime/1000, 0)
		endTime := time.Unix(job.Statistics.EndTime/1000, 0)

		stageInfo := stage.Parse(job.JobReference.JobId)
		info := &Info{
			ProjectID:    job.JobReference.ProjectId,
			JobID:        job.Id,
			StartTime:    startTime,
			EndTime:      endTime,
			TimeTakenMs: int(job.Statistics.EndTime -job.Statistics.StartTime),
			JobType:      stageInfo.Action,
			DestinationTable:        stageInfo.DestTable,
		}
		data, err := json.Marshal(info)
		if err != nil {
			return err
		}
		infoList = append(infoList, strings.TrimSpace(string(data)))
	}
	response.JobCount = len(infoList)

	if request.DestURL != "" {
		fs := afs.New()
		destURL := url.Join(request.DestURL, fmt.Sprintf("%v.json", time.Now().UnixNano()))
		fs.Upload(ctx, destURL, file.DefaultFileOsMode, bytes.NewBufferString(strings.Join(infoList, "\n")))
	}

	return nil
}


func New() Service {
	return &service{}
}

