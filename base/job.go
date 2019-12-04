package base

import (
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Job represents a big query job
type Job bigquery.Job

//Type returns job type
func (e *Job) Type() string {
	return e.Configuration.JobType
}

//Source returns job source
func (e *Job) Source() string {
	if e.Configuration == nil {
		return ""
	}
	switch e.Configuration.JobType {
	case "QUERY":
		return strings.Replace(e.Configuration.Query.Query, "\n", " ", strings.Count(e.Configuration.Query.Query, "\n"))
	case "LOAD":
		if e.Configuration.Load != nil {
			return strings.Join(e.Configuration.Load.SourceUris, ",")
		}
	case "EXTRACT":
		source := e.Configuration.Extract.SourceTable
		return source.ProjectId + ":" + source.DatasetId + "." + source.TableId
	case "COPY":
		source := e.Configuration.Copy.SourceTable
		return source.ProjectId + ":" + source.DatasetId + "." + source.TableId
	}
	return ""
}

//GetJobID returns Job ID
func (e *Job) JobID() string {
	if e.JobReference != nil {
		return e.JobReference.JobId
	}
	index := strings.Index(e.Id, ".")
	if index == -1 {
		return e.Id
	}
	return string(e.Id[index:])
}

//Table returns job destination
func (e *Job) Dest() string {
	switch e.Configuration.JobType {
	case "QUERY":
		dest := e.Configuration.Query.DestinationTable
		if dest == nil {
			return ""
		}
		return dest.ProjectId + ":" + dest.DatasetId + "." + dest.TableId
	case "LOAD":
		dest := e.Configuration.Load.DestinationTable
		if dest == nil {
			return ""
		}
		return dest.ProjectId + ":" + dest.DatasetId + "." + dest.TableId
	case "EXTRACT":
		return e.Configuration.Extract.DestinationUri
	case "COPY":
		source := e.Configuration.Copy.DestinationTable
		return source.ProjectId + ":" + source.DatasetId + "." + source.TableId
	}
	return ""
}


//SourceTable returns dest table
func (e *Job) SourceTable() string {
	if e.Configuration == nil {
		return ""
	}
	switch e.Configuration.JobType {
	case "QUERY":
		return ""
	case "LOAD":
		return ""
	case "EXTRACT":
		return ""
	case "COPY":
		dest := e.Configuration.Copy.SourceTable
		return dest.DatasetId + "." + dest.TableId
	}
	return ""
}

//DestTable returns dest table
func (e *Job) DestTable() string {
	if e.Configuration == nil {
		return ""
	}
	switch e.Configuration.JobType {
	case "QUERY":
		dest := e.Configuration.Query.DestinationTable
		if dest == nil {
			return ""
		}
		return dest.DatasetId + "." + dest.TableId
	case "LOAD":
		dest := e.Configuration.Load.DestinationTable
		if dest == nil {
			return ""
		}
		return dest.DatasetId + "." + dest.TableId
	case "EXTRACT":
		return ""
	case "COPY":
		dest := e.Configuration.Copy.DestinationTable
		return dest.DatasetId + "." + dest.TableId
	}
	return ""
}

//Error returns job error or nil
func (e *Job) Error() error {
	job := bigquery.Job(*e)
	return JobError(&job)
}

//JobError check job status and returns error or nil
func JobError(job *bigquery.Job) error {
	if job == nil {
		return nil
	}
	if job.Status != nil && job.Status.ErrorResult != nil {
		JSON, _ := json.Marshal(job.Status.Errors)
		return fmt.Errorf("failed to run job [%v]: %s, %s", job.Id, job.Status.ErrorResult.Message, JSON)
	}
	return nil
}


