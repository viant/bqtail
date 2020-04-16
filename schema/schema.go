package schema

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
	"time"
)

//New creates a schema fields from data structure
func New(aMap map[string]interface{}, description string) ([]*bigquery.TableFieldSchema, error) {
	var result = make([]*bigquery.TableFieldSchema, 0)
	if len(aMap) == 0 {
		return result, nil
	}
	for k, v := range aMap {
		field := &bigquery.TableFieldSchema{
			Name: k,
		}
		result = append(result, field)
		dataType, isRepeated := FieldType(v)
		field.Type = dataType
		if isRepeated {
			field.Mode = ModeRepeated
		}
		if strings.Contains(description, "%s") {
			timeLiteral := time.Now().Format(time.RFC3339)
			field.Description = fmt.Sprintf(description, timeLiteral)
		}
		if field.Type == FieldTypeRecord {
			if !isRepeated {
				fields, err := New(v.(map[string]interface{}), description)
				if err != nil {
					return nil, err
				}
				field.Fields = fields
				continue
			}
			aSlice := v.([]interface{})
			aSliceFields := make([][]*bigquery.TableFieldSchema, 0)
			for _, item := range aSlice {
				fields, err := New(item.(map[string]interface{}), description)
				if err != nil {
					return nil, err
				}
				aSliceFields = append(aSliceFields, fields)
			}
			field.Fields = MergeFields(aSliceFields...)
		}
	}
	return result, nil
}

//CanCopy returns true if can copy to dest
func CanCopy(source, dest *bigquery.Table) bool {
	if dest == nil || source == nil || dest.Schema == nil || source.Schema == nil {
		return true
	}
	sourceColumns := IndexFields(source.Schema.Fields)
	destColumns := IndexFields(dest.Schema.Fields)
	for k := range destColumns {
		delete(sourceColumns, k)
	}
	return len(sourceColumns) == 0
}

//IndexFields index fields
func IndexFields(schemaFields []*bigquery.TableFieldSchema) map[string]bool {
	result := make(map[string]bool)
	if len(schemaFields) == 0 {
		return result
	}
	for _, field := range schemaFields {
		result[field.Name] = true
	}
	return result
}

//MergeFields merge multi schema fields
func MergeFields(schemaFields ...[]*bigquery.TableFieldSchema) []*bigquery.TableFieldSchema {
	var merged = make(map[string]int)
	var result = make([]*bigquery.TableFieldSchema, 0)
	if len(schemaFields) == 1 {
		return schemaFields[0]
	}
	for k := range schemaFields {
		for i, field := range schemaFields[k] {
			index, ok := merged[field.Name]
			if !ok {
				merged[field.Name] = i
				result = append(result, schemaFields[k][i])
				continue
			}
			result[index].Fields = MergeFields(result[index].Fields, field.Fields)
		}
	}
	return result
}

//FieldType big query a schema returns a field type
func FieldType(v interface{}) (fieldType string, repeated bool) {
	switch val := v.(type) {
	case float64:
		fieldType = FieldTypeFloat
		if (val/10)*10 == val {
			fieldType = FieldTypeInt
		}
	case int, uint, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		fieldType = FieldTypeInt
	case bool:
		fieldType = FieldTypeBool
	case []interface{}:
		if len(val) > 0 {
			fieldType, _ = FieldType(val[0])
		}
		repeated = true
	case map[string]interface{}:
		fieldType = FieldTypeRecord
	default:
		fieldType = FieldTypeString
	}
	return fieldType, repeated
}
