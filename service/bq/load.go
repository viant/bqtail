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
	Append bool
	Request
}

const maxJobLoadURIs = 10000

//Load loads data into BigQuery
func (s *service) Load(ctx context.Context, request *LoadRequest) (job *bigquery.Job, err error) {
	projectID := request.ProjectID
	if projectID == "" {
		projectID = s.ProjectID
	}
	request.Init(projectID)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	job = &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: request.JobConfigurationLoad,
		},
	}
	job.JobReference = request.jobReference()
	job.Configuration.Load.SourceUris = s.getUniqueURIs(ctx, job.Configuration.Load.SourceUris)
	if len(job.Configuration.Load.SourceUris) <= maxJobLoadURIs {
		return s.Post(ctx, request.DestinationTable.ProjectId, job, request.PostActions())
	}
	return s.loadInParts(ctx, job, request)
}

func (s *service) getUniqueURIs(ctx context.Context, candidates []string) []string {
	var result = make([]string, 0)
	var unique = map[string]bool{}
	for i := range candidates {
		if unique[candidates[i]] {
			continue
		}
		unique[candidates[i]] = true
		result = append(result, candidates[i])
	}
	return result
}

func (s *service) loadInParts(ctx context.Context, job *bigquery.Job, request *LoadRequest) (postJob *bigquery.Job, err error) {
	URIs := job.Configuration.Load.SourceUris
	parts := len(URIs) / maxJobLoadURIs
	offset := 0
	jobID := job.JobReference.JobId
	job.Configuration.Load.WriteDisposition = "WRITE_APPEND"
	for i := 0; i < parts; i++ {
		limit := offset + maxJobLoadURIs
		if limit >= len(URIs) {
			limit = len(URIs) - 1
		}

		job.Configuration.Load.SourceUris = URIs[offset:limit]
		job.JobReference.JobId = fmt.Sprintf("j%03d_%v", i, jobID)
		if postJob, err = s.Post(ctx, request.DestinationTable.ProjectId, job, &request.Actions); err != nil {
			return nil, err
		}
	}
	return postJob, err
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
	} else if table.ProjectId != "" {
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
	if r.SourceFormat == "AVRO" {
		r.UseAvroLogicalTypes = true
	}

	if r.WriteDisposition == "" {
		if r.Append {
			r.WriteDisposition = "WRITE_APPEND"
		}
	}
	if r.CreateDisposition == "" {
		r.CreateDisposition = "CREATE_IF_NEEDED"
	}
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
