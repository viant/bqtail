package bq

import (
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//LoadRequest represents a load job
type LoadRequest struct {
	*bigquery.JobConfigurationLoad
	Request
	Append bool
}

//Load loads data into BigQuery
func (s *service) Load(ctx context.Context, request *LoadRequest) (*bigquery.Job, error) {
	request.Init(s.projectID)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: request.JobConfigurationLoad,
		},
	}
	job.JobReference = request.jobReference()
	return s.Post(ctx, request.DestinationTable.ProjectId, job, &request.Actions)
}

//Init initialises request
func (r *LoadRequest) Init(projectID string) {
	table := r.JobConfigurationLoad.DestinationTable
	if table == nil {
		return
	}

	if r.ProjectID != "" {
		projectID = r.ProjectID
	}

	if table.ProjectId == "" {
		table.ProjectId = projectID
	} else {
		projectID = table.ProjectId
	}

	if r.ProjectID == "" {
		r.ProjectID = projectID
	}

	if len(r.SourceUris) > 0 {
		sourceURI := strings.ToLower(r.SourceUris[0])
		if strings.Contains(sourceURI, ".csv") {
			r.SourceFormat = "CSV"
		} else if strings.Contains(sourceURI, ".avro") {
			r.SourceFormat = "AVRO"
		} else if strings.Contains(sourceURI, ".json") {
			r.SourceFormat = "NEWLINE_DELIMITED_JSON"
		}
	}
	if r.Append {
		r.WriteDisposition = "WRITE_APPEND"
	}
	r.CreateDisposition = "CREATE_IF_NEEDED"
}

//Validate checks if request is valid
func (r *LoadRequest) Validate() error {
	table := r.JobConfigurationLoad.DestinationTable
	if table == nil {
		return fmt.Errorf("destinationTable was empty")
	}
	if table.TableId == "" {
		return fmt.Errorf("destinationTable.TableId was empty")
	}
	if table.DatasetId == "" {
		return fmt.Errorf("destinationTable.DatasetId was empty")
	}

	if len(r.SourceUris) == 0 {
		return fmt.Errorf("sourceUris were empty")
	}
	return nil
}
