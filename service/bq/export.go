package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Export export table data to google fs
func (s *service) Export(ctx context.Context, request *ExportRequest, action *task.Action) (*bigquery.Job, error) {
	if err := request.Init(s.projectID, action); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}

	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Extract: &bigquery.JobConfigurationExtract{
				SourceTable:       request.sourceTable,
				DestinationUris:   []string{request.DestURL},
				PrintHeader:       request.IncludeHeader,
				Compression:       request.Compression,
				FieldDelimiter:    request.FieldDelimiter,
				DestinationFormat: request.Format,
			},
		},
	}
	if request.UseAvroLogicalTypes != nil {
		job.Configuration.Extract.UseAvroLogicalTypes = *request.UseAvroLogicalTypes
	}
	job.JobReference = action.JobReference()
	return s.Post(ctx, job, action)
}

//ExportRequest represents an export request
type ExportRequest struct {
	Source              string
	sourceTable         *bigquery.TableReference
	DestURL             string
	ProjectID           string
	IncludeHeader       *bool
	Compression         string
	FieldDelimiter      string
	Format              string
	UseAvroLogicalTypes *bool
}

//Init initialises request
func (r *ExportRequest) Init(projectID string, activity *task.Action) (err error) {
	activity.Meta.GetOrSetProject(projectID)
	if r.Source != "" {
		if r.sourceTable, err = base.NewTableReference(r.Source); err != nil {
			return err
		}
	}
	if r.sourceTable != nil && r.sourceTable.ProjectId == "" {
		r.sourceTable.ProjectId = projectID
	}
	if strings.ToUpper(r.Format) == "CSV" && r.FieldDelimiter == "" {
		r.FieldDelimiter = ","
	}
	if r.Format == "" {
		if strings.Contains(r.DestURL, ".json") {
			r.Format = "NEWLINE_DELIMITED_JSON"
		} else if strings.Contains(r.DestURL, ".avro") {
			r.Format = "AVRO"
		}
		if r.FieldDelimiter != "" {
			r.Format = "CSV"
		}
	}
	if r.Format == "AVRO" && r.UseAvroLogicalTypes == nil {
		set := true
		r.UseAvroLogicalTypes = &set
	}

	if r.Compression == "" {
		if strings.Contains(r.DestURL, ".gz") {
			r.Compression = "GZIP"
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *ExportRequest) Validate() error {
	if r.sourceTable == nil {
		return fmt.Errorf("sourceTable was empty")
	}
	if r.DestURL == "" {
		return fmt.Errorf("destURL was empty")
	}
	if r.Format == "" {
		return fmt.Errorf("format was empty")
	}
	return nil
}
