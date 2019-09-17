package bq

import (
	"bqtail/base"
	"context"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func (s *service) Export(ctx context.Context, request *ExportRequest) (*bigquery.Job, error) {
	if err := request.Init(s.projectID); err != nil {
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
	job.JobReference = request.jobReference()
	return s.Post(ctx, request.ProjectID, job, &request.Actions)
}

type ExportRequest struct {
	Source         string
	sourceTable    *bigquery.TableReference
	DestURL        string
	ProjectID      string
	IncludeHeader  *bool
	Compression    string
	FieldDelimiter string
	Request
	Format string
}

func (r *ExportRequest) Init(projectID string) (err error) {
	if r.ProjectID != "" {
		projectID = r.ProjectID
	} else {
		r.ProjectID = projectID
	}
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

	if r.Compression == "" {
		if strings.Contains(r.DestURL, ".gz") {
			r.Compression = "GZIP"
		}
	}
	return nil
}

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
