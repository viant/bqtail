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

//JobID returns Job ID
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

//Dest returns job destination
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

//ChildJobID return file job ID
func (e *Job) ChildJobID(childID string) string {
	result := e.JobID()
	if index := strings.Index(result, DispatchJob); index != -1 {
		result = string(result[:index])
	}
	result += childID + PathElementSeparator + DispatchJob
	return result
}

//EventID returns event ID
func (e *Job) EventID() string {
	if e.Id == "" {
		return ""
	}
	elements := strings.Split(e.JobID(), PathElementSeparator)
	for i, candidate := range elements {
		if i > 0 && (candidate == DispatchJob || candidate == TailJob) {
			candidate = elements[i-1]
			if !strings.Contains(candidate, "_") {
				return candidate
			}
			if i > 1 {
				return elements[i-2]
			}
		}
	}
	return e.JobID()
}

//SourceTable returns dest table
func (e *Job) SourceTable() string {
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
	if job.Status.ErrorResult != nil {
		JSON, _ := json.Marshal(job.Status.Errors)
		return fmt.Errorf("failed to run job: %s, %s", job.Status.ErrorResult.Message, JSON)
	}
	return nil
}

//DecodePathSeparator decode job ID
func DecodePathSeparator(jobID string) string {
	if count := strings.Count(jobID, PathElementSeparator); count > 0 {
		jobID = strings.Replace(jobID, PathElementSeparator, "/", count)
	}

	return jobID
}

//EncodePathSeparator encodes job ID
func EncodePathSeparator(jobID string) string {
	if count := strings.Count(jobID, "/"); count > 0 {
		jobID = strings.Replace(jobID, "/", PathElementSeparator, count)
	}
	if count := strings.Count(jobID, "$"); count > 0 {
		jobID = strings.Replace(jobID, "$", "_", count)
	}
	if count := strings.Count(jobID, ":"); count > 0 {
		jobID = strings.Replace(jobID, ":", "_", count)
	}
	return jobID
}
