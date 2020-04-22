package bq

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)

func (s *service) fetchAll(ctx context.Context, projectID string, SQL string) ([]map[string]bigquery.JsonValue, error) {
	useLegacy := false
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &bigquery.JobConfigurationQuery{
				Query:        SQL,
				UseLegacySql: &useLegacy,
			},
		},
	}
	jobService := bigquery.NewJobsService(s.Service)
	call := jobService.Insert(projectID, job)
	call.Context(ctx)
	job, err := call.Do()
	if err != nil {
		return nil, err
	}
	var records = []map[string]bigquery.JsonValue{}
	queryResultCall := s.Jobs.GetQueryResults(job.JobReference.ProjectId, job.JobReference.JobId)
	queryResultCall.Context(ctx)
	pageToken := ""
	var fields []*bigquery.TableFieldSchema
	for {
		var response *bigquery.GetQueryResultsResponse
		queryResultCall.PageToken(pageToken)
		err = base.RunWithRetries(func() error {
			response, err = queryResultCall.Do()
			return err
		})
		if err != nil {
			return nil, err
		}
		if fields == nil && response.Schema != nil {
			fields = response.Schema.Fields
		}
		err = s.transferRows(response, fields, &records)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to transfer record for SQL: %v", SQL)
		}
		pageToken = response.PageToken
		if pageToken == "" {
			break
		}
	}
	return records, nil
}

func (s *service) transferRows(response *bigquery.GetQueryResultsResponse, fields []*bigquery.TableFieldSchema, records *[]map[string]bigquery.JsonValue) error {
	if len(response.Rows) == 0 {
		return nil
	}
	for _, row := range response.Rows {
		var record = map[string]bigquery.JsonValue{}
		for i, cell := range row.F {
			value, err := convertValue(cell.V, fields[i])
			if err != nil {
				return err
			}
			record[fields[i].Name] = value
		}
		*records = append(*records, record)
	}
	return nil
}

func convertValue(value interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch typedValue := value.(type) {
	case []interface{}:
		return convertRepeated(typedValue, field)
	case map[string]interface{}:
		return convertNested(typedValue, field)
	}

	switch strings.ToUpper(field.Type) {
	case "INTEGER":
		return toolbox.ToInt(value)
	case "FLOAT":
		return toolbox.ToFloat(value)
	case "TIMESTAMP":
		timestampFloat, err := toolbox.ToFloat(value)
		if err != nil {
			return nil, err
		}
		timestamp := int64(timestampFloat*1000) * int64(time.Millisecond)
		timeValue := time.Unix(0, timestamp)
		return timeValue, nil
	case "BOOLEAN":
		return toolbox.AsBoolean(value), nil
	}
	return value, nil
}

func convertNested(value map[string]interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	_, ok := value["f"]
	if !ok {
		return nil, fmt.Errorf("invalid nested for field: %v", field.Name)
	}
	nested, ok := value["f"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid nested nested type for field: %v", field.Name)
	}
	var fields = field.Fields
	if len(nested) != len(fields) {
		return nil, fmt.Errorf("schema length does not match nested length for field: %v", field.Name)
	}
	var result = map[string]interface{}{}
	for i, cell := range nested {
		cellValue, ok := cell.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid nested nested item type, expected map[string]interface{}, but had %T", cellValue)
		}
		converted, err := convertValue(cellValue["v"], fields[i])
		if err != nil {
			return nil, err
		}
		result[fields[i].Name] = converted
	}
	return result, nil
}

func convertRepeated(value []interface{}, field *bigquery.TableFieldSchema) (interface{}, error) {
	var result = []interface{}{}
	for _, item := range value {
		itemValue, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid repeated type, expected map[string]inerface{}, but had %T", item)
		}
		converted, err := convertValue(itemValue["v"], field)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)

	}
	return result, nil
}
