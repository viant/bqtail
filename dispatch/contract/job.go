package contract

import (
	"bqtail/service/bq"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

type Job bigquery.Job


//Type returns job type
func (e *Job) Type() string {
	return e.Configuration.JobType
}

//Source returns job source
func (e *Job) Source() string {
	switch e.Configuration.JobType {
	case "QUERY":
		return strings.Replace(e.Configuration.Query.Query, "\n", " ", strings.Count(e.Configuration.Query.Query,"\n"))
	case "LOAD":
		return ""
	case "EXTRACT":
		source := e.Configuration.Extract.SourceTable
		return source.ProjectId + ":" + source.DatasetId + "." + source.TableId
	case "COPY":
		source := e.Configuration.Copy.SourceTable
		return source.ProjectId + ":" + source.DatasetId + "." + source.TableId
	}
	return ""
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



//DestTable returns dest table
func (e *Job) DestTable() string {
	switch e.Configuration.JobType {
	case "QUERY":
		dest := e.Configuration.Query.DestinationTable
		if dest == nil {
			return ""
		}
		return  dest.TableId
	case "LOAD":
		dest := e.Configuration.Load.DestinationTable
		if dest == nil {
			return ""
		}
		return  dest.TableId
	case "EXTRACT":
		return ""
	case "COPY":
		source := e.Configuration.Copy.DestinationTable
		return  source.TableId
	}
	return ""
}

//Error returns job error or nil
func (e *Job) Error() error {
	job := bigquery.Job(*e)
	return bq.JobError(&job)
}
