package sql

import (
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/schema"
	"github.com/viant/bqtail/tail/config"
	"google.golang.org/api/bigquery/v2"
	"sort"
	"strings"
)

func buildNestedDedupeSQL(sourceTable string, schema Schema, dest *config.Destination, except map[string]bool) string {
	projection := make([]string, 0)
	outerProjection := make([]string, 0)

	transform, transformKeys := getTransform(dest)
	for _, field := range schema.Fields {
		projection = append(projection, field.Name)

		if expression, ok := getTransformExpression(dest, field); ok {
			delete(transform, strings.ToLower(field.Name))
			outerProjection = append(outerProjection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		if except[field.Name] {
			continue
		}
		outerProjection = append(outerProjection, fmt.Sprintf("%v.%v AS %v", dest.Transient.Alias, field.Name, field.Name))
	}
	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if !ok {
			continue
		}
		projection = append(projection, key)
		outerProjection = append(outerProjection, fmt.Sprintf("%v AS %v", expression, key))
	}

	return fmt.Sprintf(`SELECT %v
FROM (
  SELECT
      %v,
      ROW_NUMBER() OVER (PARTITION BY %v) row_number
  FROM %v $WHERE
) %v $JOIN
WHERE row_number = 1`, strings.Join(outerProjection, ", "), strings.Join(projection, ", "), strings.Join(dest.UniqueColumns, ","), sourceTable, dest.Transient.Alias)
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

func columnNames(schema *bigquery.TableSchema, dest *config.Destination, except map[string]bool) []string {
	var result = make([]string, 0)

	transform, transformKeys := getTransform(dest)

	for _, field := range schema.Fields {
		if _, ok := transform[strings.ToLower(field.Name)]; ok {
			delete(transform, strings.ToLower(field.Name))
		}
		if except[field.Name] {
			continue
		}
		result = append(result, field.Name)
	}

	for _, transformKey := range transformKeys {
		if _, ok := transform[strings.ToLower(transformKey)]; !ok {
			continue
		}
		result = append(result, transformKey)
	}

	return result
}

func buildDedupeSQL(sourceTable string, schema Schema, unique map[string]bool, dest *config.Destination, except map[string]bool) string {
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
		if except[field.Name] {
			continue
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", expression, field.Name))
	}

	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if !ok {
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

func buildSelectAll(sourceTable string, schema Schema, dest *config.Destination, except map[string]bool) string {
	var projection = make([]string, 0)
	transform, transformKeys := getTransform(dest)
	for _, field := range schema.Fields {
		if except[field.Name] {
			continue
		}
		if expression, ok := getTransformExpression(dest, field); ok {
			delete(transform, strings.ToLower(field.Name))
			projection = append(projection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		projection = append(projection, fmt.Sprintf("%v.%v AS %v", dest.Transient.Alias, field.Name, field.Name))
	}
	for _, key := range transformKeys {
		expression, ok := transform[strings.ToLower(key)]
		if !ok {
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

//BuildAppendDML returns INSERT INTO table () SELECT ...
func BuildAppendDML(source, destination *bigquery.TableReference, sourceSchema *bigquery.TableSchema, dest *config.Destination, destSchema *bigquery.TableSchema) string {
	selectALL := BuildSelect(source, sourceSchema, dest, destSchema)
	except := columnExclusion(sourceSchema, destSchema)
	columns := columnNames(sourceSchema, dest, except)
	destTable := base.EncodeTableReference(destination, true)
	result := fmt.Sprintf("INSERT INTO %v(%v) %v", destTable, strings.Join(columns, ","), selectALL)
	return result
}

//BuildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func BuildSelect(source *bigquery.TableReference, sourceSchema *bigquery.TableSchema, dest *config.Destination, destSchema *bigquery.TableSchema) string {
	except := columnExclusion(sourceSchema, destSchema)
	SQL := buildSelect(source, sourceSchema, dest, except)
	join := buildJoins(dest.SideInputs)
	SQL = strings.Replace(SQL, "$JOIN", join, 1)
	return SQL
}

func columnExclusion(source, destSchema *bigquery.TableSchema) map[string]bool {
	if destSchema == nil {
		return map[string]bool{}
	}
	var excludeMap = make(map[string]bool)
	sourceMap := schema.IndexFields(source.Fields)
	destMap := schema.IndexFields(destSchema.Fields)
	for k := range sourceMap {
		if _, ok := destMap[k]; !ok {
			excludeMap[k] = true
		}
	}
	return excludeMap

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
		joinKind := "LEFT"
		if sideInput.Inner {
			joinKind = ""
		}
		joins = append(joins, fmt.Sprintf(" %v JOIN %v %v ON %v", joinKind, table, sideInput.Alias, sideInput.On))
	}
	return strings.Join(joins, "\n  ")
}

//buildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func buildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, dest *config.Destination, except map[string]bool) string {
	sourceTable := "`" + base.EncodeTableReference(source, true) + "`"
	schema := Schema(*tableScheme)
	if len(dest.UniqueColumns) == 0 {
		return buildSelectAll(sourceTable, schema, dest, except)
	}
	if schema.IsNested() {
		return buildNestedDedupeSQL(sourceTable, schema, dest, except)
	}
	var unique = make(map[string]bool)
	for _, column := range dest.UniqueColumns {
		unique[strings.ToLower(column)] = true
	}
	return buildDedupeSQL(sourceTable, schema, unique, dest, except)
}
