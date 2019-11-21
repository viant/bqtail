package tail

import (
	"bqtail/base"
	"bqtail/task"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

const (
	recoverJobPrefix = "recover"
	notFoundReason = "notFound"
)

//wrapRecoverJobID wrap recover with recover prefix and attempts
func wrapRecoverJobID(jobID string) string {
	attempt := 1
	if strings.HasPrefix(jobID, recoverJobPrefix) {
		offset := len(recoverJobPrefix)
		if offset + 4 < len(jobID) {
			attemptCounter := string(jobID[offset : offset +4])
			attempt = toolbox.AsInt(attemptCounter) +1
		}
		jobID = string(jobID[offset +5:])
	}
	return fmt.Sprintf(recoverJobPrefix + "%04d_%v", attempt, jobID)
}




func removeCorruptedURIs(ctx context.Context, job *bigquery.Job, fs afs.Service) (corrupted, missing []string, valid []string) {
	var URIs = make(map[string]bool)
	for _, URI := range job.Configuration.Load.SourceUris {
		URIs[URI] = true
	}
	corrupted = make([]string, 0)
	missing = make([]string, 0)
	for _, element := range job.Status.Errors {
		isMissing := false
		if element.Reason == notFoundReason && element.Location == "" {
			element.Message = strings.Replace(element.Message, "/bigstore", "gs:/", 1)
			if index := strings.Index(element.Message, "gs://");index !=-1 {
				element.Location = string(element.Message[index:])
				isMissing = true
			}
		}
		if element.Location == "" {
			continue
		}
		if _, ok := URIs[element.Location]; ! ok {
			continue
		}
		delete(URIs, element.Location)
		if isMissing {
			missing = append(missing, element.Location)
			continue
		}
		corrupted = append(corrupted, element.Location)
	}
	valid = make([]string, 0)
	for URI := range URIs {
		if fs != nil {
			if ok, _ := fs.Exists(ctx,URI); !ok {
				continue
			}
		}

		valid = append(valid, URI)
	}
	return corrupted, missing, valid
}


func updateJobID(eventID, jobID string) string {
	elements := strings.Split(jobID, "/")
	if len(elements) > 2 {
		previousEventID := elements[len(elements)-2]
		return strings.Replace(jobID, previousEventID, eventID, len(jobID))
	}
	return eventID + jobID
}

func buildJobIDReplacementMap(eventID string, actions []*task.Action) map[string]interface{} {
	var result = make(map[string]interface{})
	for _, action := range actions {
		jobID, ok := action.Request[base.JobIDKey]
		if ok {
			result[base.JobIDKey] = updateJobID(eventID, toolbox.AsString(jobID))
			break
		}
	}
	return result
}