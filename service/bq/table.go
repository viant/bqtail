package bq

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"net/http"
	"strings"
)

//Table returns bif query table
func (s *service) Table(ctx context.Context, reference *bigquery.TableReference) (table *bigquery.Table, err error) {
	if reference.ProjectId == "" {
		reference.ProjectId = s.projectID
	}
	tableID := base.TableID(reference.TableId)
	call := bigquery.NewTablesService(s.Service).Get(reference.ProjectId, reference.DatasetId, tableID)
	call.Context(ctx)
	err = base.RunWithRetries(func() error {
		table, err = call.Do()
		if isAlreadyExistError(err) {
			err = nil
		}
		return err
	})
	if err != nil {
		err = errors.Wrapf(err, "failed to lookup table schema: %v:%v.%v", reference.ProjectId, reference.DatasetId, tableID)
	}
	return table, err
}

func isAlreadyExistError(err error) bool {
	if err == nil {
		return false
	}
	if apiError, ok := err.(*googleapi.Error); ok {
		if apiError.Code == http.StatusConflict { //already exists
			return true
		}
	}
	return strings.Contains(err.Error(), "Already Exists")
}

//CreateTableIfNotExist creates a table if does not exist
func (s *service) CreateTableIfNotExist(ctx context.Context, table *bigquery.Table, patchIfDifferent bool) error {
	ref := table.TableReference
	srv := bigquery.NewTablesService(s.Service)
	if ref.ProjectId == "" {
		ref.ProjectId = s.ProjectID
	}
	getTableCall := srv.Get(ref.ProjectId, ref.DatasetId, ref.TableId)
	getTableCall.Context(ctx)
	existing, err := getTableCall.Do()
	if !base.IsNotFoundError(err) && !patchIfDifferent {
		return nil
	}

	if existing != nil {
		isEqual := isSchemaEqual(existing.Schema.Fields, table.Schema.Fields)
		if isEqual {
			return nil
		}
		patchable := isSchemaPatchable(existing.Schema.Fields, table.Schema.Fields)
		if patchable {
			if isEqual {
				return nil
			}
			_, err = s.Patch(ctx, &PatchRequest{
				Table:         base.EncodeTableReference(table.TableReference, false),
				TemplateTable: table,
				ProjectID:     ref.ProjectId,
			})
			return err
		}
		return nil
	}

	if shared.IsDebugLoggingLevel() {
		shared.LogF("create table: %+v\n", table.TableReference)
		shared.LogLn(table)
	}
	insertTableCall := srv.Insert(ref.ProjectId, ref.DatasetId, table)
	insertTableCall.Context(ctx)
	return base.RunWithRetries(func() error {
		_, err = insertTableCall.Do()
		return err
	})
}

func isSchemaEqual(source []*bigquery.TableFieldSchema, template []*bigquery.TableFieldSchema) bool {
	if len(source) != len(template) {
		return false
	}
	sourceIndex := map[string]*bigquery.TableFieldSchema{}
	for i := range source {
		sourceIndex[source[i].Name] = source[i]
	}
	for _, dest := range template {
		src, ok := sourceIndex[dest.Name]
		if !ok {
			return false
		}
		if src.Type != dest.Type {
			return false
		}
		if len(src.Fields) > 0 {
			if !isSchemaEqual(src.Fields, dest.Fields) {
				return false
			}
		}
	}
	return true
}

func isSchemaPatchable(source []*bigquery.TableFieldSchema, template []*bigquery.TableFieldSchema) bool {
	if len(source) > len(template) {
		index := indexFields(template)
		for _, f := range source {
			if _, ok := index[f.Name]; !ok {
				if shared.IsDebugLoggingLevel() {
					shared.LogF("not patchable: template is missing %v\n", f.Name)
				}
			}
		}
		return false
	}
	index := indexFields(source)
	for _, dest := range template {
		src, ok := index[dest.Name]
		if !ok {
			continue
		}
		if src.Type != dest.Type {
			return false
		}
		if len(src.Fields) > 0 {
			if !isSchemaPatchable(src.Fields, dest.Fields) {
				return false
			}
		}
	}
	return true
}

func indexFields(source []*bigquery.TableFieldSchema) map[string]*bigquery.TableFieldSchema {
	index := map[string]*bigquery.TableFieldSchema{}
	for i := range source {
		index[source[i].Name] = source[i]
	}
	return index
}
