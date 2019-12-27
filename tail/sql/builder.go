package sql

import (
	"bqtail/base"
	"bqtail/tail/config"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func buildNestedDedupeSQL(sourceTable string, schema Schema, uniqueColumns []string, transform map[string]string, tableAlias string) string {
	projection := make([]string, 0)
	innerProjection := make([]string, 0)

	for _, field := range schema.Fields {
		projection = append(projection, field.Name)
		if expression, ok := transform[strings.ToLower(field.Name)]; ok {
			innerProjection = append(innerProjection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}
		innerProjection = append(innerProjection, fmt.Sprintf("%v.%v AS %v", tableAlias, field.Name, field.Name))
	}
	return fmt.Sprintf(`SELECT %v
FROM (
  SELECT
      %v,
      ROW_NUMBER() OVER (PARTITION BY %v) row_number
  FROM %v $WHERE
) %v $JOIN
WHERE row_number = 1`, strings.Join(innerProjection, ", "), strings.Join(projection, ", "), strings.Join(uniqueColumns, ","), sourceTable, tableAlias)
}

func buildDedupeSQL(sourceTable string, schema Schema, unique map[string]bool, transform map[string]string, tableAlias string) string {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)
	for i, field := range schema.Fields {
		if unique[strings.ToLower(field.Name)] {
			groupBy = append(groupBy, fmt.Sprintf("%d", i+1))
			projection = append(projection, field.Name)
			continue
		}
		expression := fmt.Sprintf("%v.%v", tableAlias, field.Name)
		if transformExpression, ok := transform[strings.ToLower(field.Name)]; ok {
			expression = transformExpression
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", expression, field.Name))
	}
	return fmt.Sprintf(`SELECT %v 
FROM %v %v $JOIN
$WHERE
GROUP BY %v`,
		strings.Join(projection, ", "),
		sourceTable,
		tableAlias,
		strings.Join(groupBy, ", "),
	)
}

func buildSelectAll(sourceTable string, schema Schema, transform map[string]string, tableAlias string) string {
	var projection = make([]string, 0)
	for _, field := range schema.Fields {
		if expression, ok := transform[strings.ToLower(field.Name)]; ok {
			projection = append(projection, fmt.Sprintf("%v AS %v", expression, field.Name))
			continue
		}

		projection = append(projection, fmt.Sprintf("%v.%v AS %v", tableAlias, field.Name, field.Name))
	}
	return fmt.Sprintf(`SELECT %v 
FROM %v %v $JOIN $WHERE`,
		strings.Join(projection, ", "),
		sourceTable,
		tableAlias,
	)
}

//BuildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func BuildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, tableAlias string, uniqueColumns []string, transform map[string]string, sideInputs []*config.SideInput) string {
	SQL := buildSelect(source, tableScheme, uniqueColumns, transform, tableAlias)
	join := buildJoins(sideInputs)
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
func buildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, uniqueColumns []string, transform map[string]string, tableAlias string) string {
	tableID := base.TableID(source.TableId)
	sourceTable := source.DatasetId + "." + tableID
	schema := Schema(*tableScheme)
	if len(uniqueColumns) == 0 {
		return buildSelectAll(sourceTable, schema, transform, tableAlias)
	}
	if schema.IsNested() {
		return buildNestedDedupeSQL(sourceTable, schema, uniqueColumns, transform, tableAlias)
	}
	var unique = make(map[string]bool)
	for _, column := range uniqueColumns {
		unique[strings.ToLower(column)] = true
	}
	return buildDedupeSQL(sourceTable, schema, unique, transform, tableAlias)
}
