package sql

import "google.golang.org/api/bigquery/v2"

//Schema represents table schema
type Schema bigquery.TableSchema

//IsNested returns true if schema is nested
func (s Schema) IsNested() bool {
	if len(s.Fields) == 0 {
		return false
	}
	for _, field := range s.Fields {
		if field.Mode == "REPEATED" {
			return true
		}
		if len(field.Fields) > 0 {
			return true
		}
	}
	return false
}
