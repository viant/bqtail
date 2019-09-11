package sql

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func buildNestedDedupeSQL(sourceTable string, schema Schema, uniqueColumns []string) string {
	projection := make([]string, 0)
	for _, field := range schema.Fields {
		projection = append(projection, field.Name)
	}
	return fmt.Sprintf(`SELECT %v
FROM (
  SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY %v) row_number
  FROM %v
)
WHERE row_number = 1`, strings.Join(projection, ", "), strings.Join(uniqueColumns, ","), sourceTable)
}

func buildDedupeSQL(sourceTable string, schema Schema, unique map[string]bool) string {
	var projection = make([]string, 0)
	var groupBy = make([]string, 0)

	for i, field := range schema.Fields {
		if unique[strings.ToLower(field.Name)] {
			groupBy = append(groupBy, fmt.Sprintf("%d", i+1))
			projection = append(projection, field.Name)
			continue
		}
		projection = append(projection, fmt.Sprintf("MAX(%v) AS %v", field.Name, field.Name))
	}
	return fmt.Sprintf(`SELECT %v 
FROM %v 
GROUP BY %v`,
		strings.Join(projection, ", "),
		sourceTable,
		strings.Join(groupBy, ", "),
	)
}

//BuildSelect returns select SQL statement for specified parameter, if uniqueColumns SQL de-duplicates data
func BuildSelect(source *bigquery.TableReference, tableScheme *bigquery.TableSchema, uniqueColumns []string) string {
	sourceTable := source.DatasetId + "." + source.TableId
	if len(uniqueColumns) == 0 {
		return fmt.Sprintf("SELECT * FROM %v", sourceTable)
	}
	schema := Schema(*tableScheme)
	if schema.IsNested() {
		return buildNestedDedupeSQL(sourceTable, schema, uniqueColumns)
	}
	var unique = make(map[string]bool)
	for _, column := range uniqueColumns {
		unique[strings.ToLower(column)] = true
	}
	return buildDedupeSQL(sourceTable, schema, unique)
}
