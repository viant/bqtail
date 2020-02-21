package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//LoadRequest represents a load job
type LoadRequest struct {
	*bigquery.JobConfigurationLoad
	Append bool
}

const maxJobLoadURIs = 10000

//Load loads data into BigQuery
func (s *service) Load(ctx context.Context, request *LoadRequest, action *task.Action) (job *bigquery.Job, err error) {
	projectID := action.Meta.ProjectID
	if projectID == "" {
		projectID = s.Config.ProjectID
	}
	request.Init(projectID, action)
	if err := request.Validate(); err != nil {
		return nil, err
	}
	job = &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: request.JobConfigurationLoad,
		},
	}
	job.JobReference = action.JobReference()
	job.Configuration.Load.SourceUris = s.getUniqueURIs(ctx, job.Configuration.Load.SourceUris)
	s.adjustRegion(ctx, action, job.Configuration.Load.DestinationTable)
	datafileCount := len(job.Configuration.Load.SourceUris)
	if shared.IsInfoLoggingLevel() {
		shared.LogF("[%v] loading %v datafile(s) into %v", action.Meta.DestTable, datafileCount, base.EncodeTableReference(job.Configuration.Load.DestinationTable, true))
	}
	if datafileCount <= maxJobLoadURIs {
		return s.Post(ctx, job, action)
	}
	postJob, err := s.loadInParts(ctx, job, request, action)
	if err == nil && base.JobError(postJob) == nil {
		if shared.IsInfoLoggingLevel() {
			shared.LogF("[%v] loaded % datafile(s).\n", action.Meta.DestTable, datafileCount)
		}
	}
	return postJob, err
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

func (s *service) loadInParts(ctx context.Context, job *bigquery.Job, request *LoadRequest, Action *task.Action) (postJob *bigquery.Job, err error) {
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
		if postJob, err = s.Post(ctx, job, Action); err != nil {
			return nil, err
		}
	}
	return postJob, err
}

//Init initialises request
func (r *LoadRequest) Init(projectID string, Action *task.Action) {
	table := r.JobConfigurationLoad.DestinationTable
	if table == nil {
		return
	}
	projectID = Action.Meta.GetOrSetProject(projectID)

	if table.ProjectId == "" {
		table.ProjectId = projectID
	} else if table.ProjectId != "" {
		projectID = table.ProjectId
	}
	if Action.Meta.ProjectID == "" {
		Action.Meta.ProjectID = projectID
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
