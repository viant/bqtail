package bq

import (
	"bqtail/model"
	"context"
	"errors"
	"fmt"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//LoadRequest represents load request
type LoadRequest struct {
	JobID string
	bigquery.JobConfigurationLoad
	Wait   bool
	Append bool
}

//Init initialises request
func (r *LoadRequest) Init() {
	if r.DestinationTable != nil {
		if r.DestinationTable.ProjectId == "" {
			if credentials, err := google.FindDefaultCredentials(context.Background()); err == nil {
				r.DestinationTable.ProjectId = credentials.ProjectID
			}
		}
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
}

//Validate checks if request is valid
func (r *LoadRequest) Validate() error {
	if r.DestinationTable == nil {
		return errors.New("destinationTable was nil")
	}
	if r.DestinationTable.DatasetId == "" {
		return errors.New("destinationTable.datasetId was empty")
	}
	if r.DestinationTable.TableId == "" {
		return errors.New("destinationTable.tableId was empty")
	}
	if len(r.SourceUris) == 0 {
		return fmt.Errorf("sourceUris were empty")
	}
	return nil
}

//NewLoadRequest creates a new load request
func NewLoadRequest(jobID string, dest *model.Table, wait, append bool, URIs ...string) *LoadRequest {
	result := &LoadRequest{
		JobID:  jobID,
		Wait:   wait,
		Append: append,
	}
	result.DestinationTable = &bigquery.TableReference{}

	result.DestinationTable.TableId = dest.TableID
	result.DestinationTable.DatasetId = dest.DatasetID
	result.DestinationTable.ProjectId = dest.ProjectID
	result.SourceUris = URIs
	return result
}

//LoadResponse represents load response
type LoadResponse struct {
	*bigquery.Job
	Error string
}
