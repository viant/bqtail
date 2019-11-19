package tail

import (
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




func removeCorruptedURIs(ctx context.Context, job *bigquery.Job, fs afs.Service) (corrupted []string, valid []string) {
	var URIs = make(map[string]bool)
	for _, URI := range job.Configuration.Load.SourceUris {
		URIs[URI] = true
	}
	corrupted = make([]string, 0)
	for _, element := range job.Status.Errors {
		if element.Reason == notFoundReason && element.Location == "" {
			element.Message = strings.Replace(element.Message, "/bigstore", "gs:/", 1)
			if index := strings.Index(element.Message, "gs://");index !=-1 {
				element.Location = string(element.Message[index:])
			}
		}
		if element.Location == "" {
			continue
		}
		if _, ok := URIs[element.Location]; ! ok {
			continue
		}
		corrupted = append(corrupted, element.Location)
		delete(URIs, element.Location)
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
	return corrupted, valid
}