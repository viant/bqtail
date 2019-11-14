package sql

import (
	"bqtail/base"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func buildNestedDedupeSQL(sourceTable string, schema Schema, uniqueColumns []string, transform map[string]string) string {
	projection := make([]string, 0)
	innerProjection := make([]string, 0)

	for _, field := range schema.Fields {
		projection = append(projection, field.Name)
		if expression, ok := transform[strings.ToLower(field.Name)]; ok {
			innerProjection = append(innerProjection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		innerProjection = append(innerProjection, field.Name)
	}
	return fmt.Sprintf(`SELECT %v
FROM (
  SELECT
      %v,
      ROW_NUMBER() OVER (PARTITION BY %v) row_number
  FROM %v $WHERE
)
WHERE row_number = 1`, strings.Join(innerProjection, ", "), strings.Join(projection, ", "), strings.Join(uniqueColumns, ","), sourceTable)
}


func buildDedupeSQL(sourceTable string, schema Schema, unique map[string]bool, transform map[string]string) string {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)
	for i, field := range schema.Fields {
		if unique[strings.ToLower(field.Name)] {
			groupBy = append(groupBy, fmt.Sprintf("%d", i+1))
			projection = append(projection, field.Name)
			continue
		}
		expression := field.Name
		if transformExpression, ok := transform[strings.ToLower(field.Name)]; ok {
			expression = transformExpression
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", expression, field.Name))
	}
	return fmt.Sprintf(`SELECT %v 
FROM %v 
$WHERE
GROUP BY %v`,
		strings.Join(projection, ", "),
		sourceTable,
		strings.Join(groupBy, ", "),
	)
}



func buildSelectAll(sourceTable string, schema Schema, transform map[string]string) string {
	var projection = make([]string, 0)
	for _, field := range schema.Fields {
		if expression, ok := transform[strings.ToLower(field.Name)]; ok {
			projection = append(projection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		projection = append(projection, field.Name)
	}
	return fmt.Sprintf(`SELECT %v 
FROM %v  $WHERE`,
		strings.Join(projection, ", "),
		sourceTable,
	)
}

//BuildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func BuildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, uniqueColumns []string, transform map[string]string) string {
	tableId := base.TableID(source.TableId)
	sourceTable := source.DatasetId + "." + tableId
	schema := Schema(*tableScheme)
	if len(uniqueColumns) == 0 {
		return buildSelectAll(sourceTable, schema, transform)
	}
	if schema.IsNested() {
		return buildNestedDedupeSQL(sourceTable, schema, uniqueColumns, transform)
	}
	var unique = make(map[string]bool)
	for _, column := range uniqueColumns {
		unique[strings.ToLower(column)] = true
	}
	return buildDedupeSQL(sourceTable, schema, unique, transform)
}
