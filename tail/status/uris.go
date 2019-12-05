package status

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

const (
	notFoundReason = "notFound"
)

//URIs represents error classified URIs
type URIs struct {
	Valid         []string `json:",omitempty"`
	InvalidSchema []string `json:",omitempty"`
	Missing       []string `json:",omitempty"`
	Corrupted     []string `json:",omitempty"`
}

func (u *URIs) Classify(ctx context.Context, fs afs.Service, job *bigquery.Job) {
	var URIs = make(map[string]bool)
	for _, URI := range job.Configuration.Load.SourceUris {
		URIs[URI] = true
	}
	if job.Status == nil || len(job.Status.Errors) == 0 {
		u.Valid = job.Configuration.Load.SourceUris
		return
	}
	schemaErrors := getInvalidSchemaLocations(job)
	for _, element := range job.Status.Errors {
		isMissing := false
		if element.Reason == notFoundReason && element.Location == "" {
			element.Message = strings.Replace(element.Message, "/bigstore", "gs:/", 1)
			if index := strings.Index(element.Message, "gs://"); index != -1 {
				element.Location = string(element.Message[index:])
				isMissing = true
			}
		}
		if element.Location == "" {
			continue
		}

		if _, ok := URIs[element.Location]; !ok {
			continue
		}

		delete(URIs, element.Location)
		if schemaErrors[element.Location] {
			u.InvalidSchema = append(u.InvalidSchema, element.Location)
			continue
		}
		if isMissing {
			u.Missing = append(u.Missing, element.Location)
			continue
		}
		u.Corrupted = append(u.Corrupted, element.Location)
	}

	var valid = make([]string, 0)
	for URI := range URIs {
		if fs != nil {
			if ok, _ := fs.Exists(ctx, URI, option.NewObjectKind(true)); !ok {
				continue
			}
		}
		valid = append(valid, URI)
	}
	u.Valid = valid
}

func getInvalidSchemaLocations(job *bigquery.Job) map[string]bool {
	var schemaError = make(map[string]bool)
	for _, element := range job.Status.Errors {
		if element.Location == "" {
			continue
		}
		if strings.Contains(strings.ToLower(element.Message), "field") || strings.Contains(strings.ToLower(element.Message), "schema") {
			schemaError[element.Location] = true
		}
	}
	return schemaError
}

//NewURIs create am URIs
func NewURIs() *URIs {
	return &URIs{
		Valid:         make([]string, 0),
		InvalidSchema: make([]string, 0),
		Missing:       make([]string, 0),
		Corrupted:     make([]string, 0),
	}
}
