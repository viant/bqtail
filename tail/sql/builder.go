package sql

import (
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/tail/config"
	"google.golang.org/api/bigquery/v2"
	"sort"
	"strings"
)

func buildNestedDedupeSQL(sourceTable string, schema Schema, dest *config.Destination) string {
	projection := make([]string, 0)
	innerProjection := make([]string, 0)

	transform, transformKeys := getTransform(dest)
	for _, field := range schema.Fields {
		projection = append(projection, field.Name)
		if expression, ok := getTransformExpression(dest, field); ok {
			delete(transform, strings.ToLower(field.Name))
			innerProjection = append(innerProjection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		innerProjection = append(innerProjection, fmt.Sprintf("%v.%v AS %v", dest.Transient.Alias, field.Name, field.Name))
	}
	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if ! ok {
			continue
		}
		projection = append(projection, key)
		innerProjection = append(innerProjection, fmt.Sprintf("%v AS %v", expression, key))
	}

	return fmt.Sprintf(`SELECT %v
FROM (
  SELECT
      %v,
      ROW_NUMBER() OVER (PARTITION BY %v) row_number
  FROM %v $WHERE
) %v $JOIN
WHERE row_number = 1`, strings.Join(innerProjection, ", "), strings.Join(projection, ", "), strings.Join(dest.UniqueColumns, ","), sourceTable, dest.Transient.Alias)
}

func getTransform(dest *config.Destination) (map[string]string, []string) {
	transform := make(map[string]string)
	transformKeys := make([]string, 0)
	if len(dest.Transform) > 0 {
		for k, v := range dest.Transform {
			transform[strings.ToLower(k)] = v
			transformKeys = append(transformKeys, k)
		}
	}
	sort.Strings(transformKeys)
	return transform, transformKeys
}

func columnNames(schema *bigquery.TableSchema) []string {
	var result = make([]string, 0)
	for _, field := range schema.Fields {
		result = append(result, field.Name)
	}
	return result
}

func buildDedupeSQL(sourceTable string, schema Schema, unique map[string]bool, dest *config.Destination) string {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)


	transform, transformKeys := getTransform(dest)

	for i, field := range schema.Fields {
		if unique[strings.ToLower(field.Name)] {
			groupBy = append(groupBy, fmt.Sprintf("%d", i+1))
			projection = append(projection, field.Name)
			continue
		}
		expression := fmt.Sprintf("%v.%v", dest.Transient.Alias, field.Name)
		if transformExpression, ok := getTransformExpression(dest, field); ok {
			delete(transform, strings.ToLower(field.Name))
			expression = transformExpression
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", expression, field.Name))
	}

	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if ! ok {
			continue
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", expression, key))
	}

	return fmt.Sprintf(`SELECT %v 
FROM %v %v $JOIN
$WHERE
GROUP BY %v`,
		strings.Join(projection, ", "),
		sourceTable,
		dest.Transient.Alias,
		strings.Join(groupBy, ", "),
	)
}

func buildSelectAll(sourceTable string, schema Schema, dest *config.Destination) string {
	var projection = make([]string, 0)
	transform, transformKeys := getTransform(dest)
	for _, field := range schema.Fields {
		if expression, ok := getTransformExpression(dest, field); ok {
			delete(transform, strings.ToLower(field.Name))
			projection = append(projection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		projection = append(projection, fmt.Sprintf("%v.%v AS %v", dest.Transient.Alias, field.Name, field.Name))
	}
	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if ! ok {
			continue
		}
		projection = append(projection, fmt.Sprintf("%v AS %v", expression, key))
	}

	return fmt.Sprintf(`SELECT %v 
FROM %v %v $JOIN $WHERE`,
		strings.Join(projection, ", "),
		sourceTable,
		dest.Transient.Alias,
	)
}

func getTransformExpression(dest *config.Destination, field *bigquery.TableFieldSchema) (string, bool) {
	expression, ok := dest.Transform[field.Name]
	if !ok {
		expression, ok = dest.Transform[strings.ToLower(field.Name)]
	}
	return expression, ok
}

//BuilAppendDML returns INSERT INTO table () SELECT ...
func BuilAppendDML(source, destination *bigquery.TableReference, tableScheme *bigquery.TableSchema, dest *config.Destination) string {
	selectALL := BuildSelect(source, tableScheme, dest)
	columns := columnNames(tableScheme)
	destTable := base.EncodeTableReference(destination, true)
	return fmt.Sprintf("INSERT INTO %v(%v) %v", destTable, strings.Join(columns, ","), selectALL)
}

//BuildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func BuildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, dest *config.Destination) string {
	SQL := buildSelect(source, tableScheme, dest)
	join := buildJoins(dest.SideInputs)
	SQL = strings.Replace(SQL, "$JOIN", join, 1)
	return SQL
}

func buildJoins(sideInputs []*config.SideInput) string {
	if len(sideInputs) == 0 {
		return ""
	}
	var joins = make([]string, 0)
	for _, sideInput := range sideInputs {
		table := sideInput.Table
		if sideInput.From != "" {
			table = "(" + sideInput.From + ")"
		}
		joins = append(joins, fmt.Sprintf(" LEFT JOIN %v %v ON %v", table, sideInput.Alias, sideInput.On))
	}
	return strings.Join(joins, "\n  ")
}

//buildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func buildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, dest *config.Destination) string {
	sourceTable := "`" + base.EncodeTableReference(source, true) + "`"
	schema := Schema(*tableScheme)
	if len(dest.UniqueColumns) == 0 {
		return buildSelectAll(sourceTable, schema, dest)
	}
	if schema.IsNested() {
		return buildNestedDedupeSQL(sourceTable, schema, dest)
	}
	var unique = make(map[string]bool)
	for _, column := range dest.UniqueColumns {
		unique[strings.ToLower(column)] = true
	}
	return buildDedupeSQL(sourceTable, schema, unique, dest)
}
