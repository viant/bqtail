package status

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/bqtail/base"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

const (
	notFoundReason      = "notFound"
	noSuchFieldFragment = "No such field:"
	rowsFragment        = "Rows:"
)

//URIs represents error classified URIs
type URIs struct {
	Valid         []string `json:",omitempty"`
	InvalidSchema []string `json:",omitempty"`
	Missing       []string `json:",omitempty"`
	Corrupted     []string `json:",omitempty"`
	MissingFields []*Field `json:",omitempty"`
}

//Classify classify uri
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
	if len(schemaErrors) > 0 {
		u.addMissingFields(job)
	}

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
		var err error
		if fs != nil {
			exists := true
			err = base.RunWithRetries(func() error {
				exists, err = fs.Exists(ctx, URI, option.NewObjectKind(true))
				return err
			})
			if !exists && err == nil {
				continue
			}
		}
		valid = append(valid, URI)
	}
	u.Valid = valid
}

func (u *URIs) addMissingFields(job *bigquery.Job) {
	rows := 0
	for _, element := range job.Status.Errors {
		if index := strings.Index(element.Message, rowsFragment); index != -1 {
			rowsInfo := string(element.Message[index+1+len(rowsFragment):])
			if index := strings.Index(rowsInfo, ";"); index != -1 {
				rowsInfo = string(rowsInfo[:index])
				rows = toolbox.AsInt(strings.TrimSpace(rowsInfo))
			}
		}
		if index := strings.Index(element.Message, noSuchFieldFragment); index != -1 {
			field := strings.Trim(string(element.Message[index+1+len(noSuchFieldFragment):]), ".")
			if index := strings.Index(field, "File:"); index != -1 {
				issueLocation := field[index+len("File:"):]
				if len(issueLocation) > 0 {
					if index := strings.Index(issueLocation, "gs:"); index != -1 {
						element.Location = strings.TrimSpace(issueLocation[index:])
					}
				}
				field = field[:index]
			}
			field = strings.Trim(field, " .")
			u.MissingFields = append(u.MissingFields, &Field{
				Name:     field,
				Location: element.Location,
				Row:      rows,
				Type:     "",
			})
			break
		}
	}
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
		MissingFields: make([]*Field, 0),
	}
}
