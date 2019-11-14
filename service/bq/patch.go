package bq

import (
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Export export table data to google fs
func (s *service) Patch(ctx context.Context, request *PatchRequest) (*bigquery.Job, error) {
	if err := request.Init(s.projectID); err != nil {
		return nil, err
	}


	if err := request.Validate(); err != nil {
		return nil, err
	}




	//
	//job := &bigquery.Job{
	//	Configuration: &bigquery.JobConfiguration{
	//		Extract: &bigquery.JobConfigurationExtract{
	//			SourceTable:       request.sourceTable,
	//			DestinationUris:   []string{request.DestURL},
	//			PrintHeader:       request.IncludeHeader,
	//			Compression:       request.Compression,
	//			FieldDelimiter:    request.FieldDelimiter,
	//			DestinationFormat: request.Format,
	//		},
	//	},
	//}
	//job.JobReference = request.jobReference()
	//return s.Post(ctx, request.ProjectID, job, &request.Actions)
	return nil, nil
}




//ExportRequest represents an export request
type PatchRequest struct {
	Template string
	Destination string
	Request
}

//Init initialises request
func (r *PatchRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}
	return nil
}

//Validate checks if request is valid
func (r *PatchRequest) Validate() error {
	if r.Template == "" {
		return fmt.Errorf("template was empty")
	}
	if r.Destination == "" {
		return fmt.Errorf("destination was empty")
	}
	return nil
}


