package load

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/tail/sql"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

const (
	timestampDataType = "TIMESTAMP"
	timePartitionType = "DAY"
)

func (j *Job) addSplitActions(selectSQL string, result, onDone *task.Actions) error {
	split := j.Rule.Dest.Schema.Split
	if split == nil {
		return nil
	}

	next := onDone
	dest := j.Rule.Dest.Clone()
	destTemplate := ""
	if dest.Schema.Template != "" {
		destTemplate = dest.Schema.Template
	}

	clusterColumnMap := j.clusterColumnMap()

	for i := range split.Mapping {
		mapping := split.Mapping[i]
		destTable, _ := dest.CustomTableReference(mapping.Then, j.Process.Source)

		where := replaceWithMap(mapping.When, clusterColumnMap)
		SQL := strings.Replace(selectSQL, "$WHERE", " WHERE  "+where+" ", 1)
		query := bq.NewQueryAction(SQL, destTable, destTemplate, j.Rule.IsAppend(), next)
		group := task.NewActions(nil, nil)
		group.AddOnSuccess(query)
		next = group
	}

	if len(j.splitColumns) == 0 {
		result.AddOnSuccess(next.OnSuccess...)
		result.AddOnSuccess(next.OnFailure...)
		return nil
	}
	if len(dest.Transform) == 0 {
		dest.Transform = make(map[string]string)
	}
	for _, column := range j.splitColumns {
		dest.Transform[column.Name] = column.Type + "(NULL)"
	}
	if len(split.ClusterColumns) > 0 {
		for i, column := range split.ClusterColumns {
			if index := strings.LastIndex(split.ClusterColumns[i], "."); index != -1 {
				dest.Transform[string(column[index+1:])] = column
			}
		}
	}
	sourceRef, _ := base.NewTableReference(j.SplitTable())
	selectAll := sql.BuildSelect(sourceRef, j.SplitSchema.Schema, dest)
	selectAll = strings.Replace(selectAll, "$WHERE", "", 1)
	destRef, _ := base.NewTableReference(j.TempTable)
	result.AddOnSuccess(bq.NewQueryAction(selectAll, destRef, "", false, next))
	return nil
}

func replaceWithMap(when string, columnMap map[string]string) string {
	for k, v := range columnMap {
		count := strings.Count(when, k)
		if count == 0 {
			continue
		}
		when = strings.Replace(when, k, v, count)
	}
	return when
}

func (j *Job) clusterColumnMap() map[string]string {
	split := j.Rule.Dest.Schema.Split
	result := map[string]string{}
	if len(split.ClusterColumns) > 0 {
		for i, column := range split.ClusterColumns {
			if index := strings.LastIndex(split.ClusterColumns[i], "."); index != -1 {
				result[column] = string(column[index+1:])
			}
		}
	}
	return result
}

func (j *Job) initTableSplit(ctx context.Context, service bq.Service) error {
	split := j.Rule.Dest.Schema.Split
	if j.Load.Schema == nil {
		return nil
	}
	tableRef, _ := base.NewTableReference(j.TempTable)
	tempTable := &bigquery.Table{
		Schema:         &bigquery.TableSchema{Fields: j.Load.Schema.Fields},
		TableReference: tableRef,
	}
	splitColumns := []*bigquery.TableFieldSchema{}
	schema := tempTable.Schema
	if len(split.ClusterColumns) > 0 {
		if split.TimeColumn == "" {
			split.TimeColumn = "ts"
		}
		field := getColumn(schema.Fields, split.TimeColumn)
		if field == nil {
			splitColumns = append(splitColumns, &bigquery.TableFieldSchema{
				Name: split.TimeColumn,
				Type: timestampDataType,
			})
		}
		tempTable.TimePartitioning = &bigquery.TimePartitioning{
			Field: split.TimeColumn,
			Type:  timePartitionType,
		}
		var clusterColumn = make([]string, 0)
		for i, name := range split.ClusterColumns {
			if strings.Contains(split.ClusterColumns[i], ".") {
				column := getColumn(schema.Fields, split.ClusterColumns[i])
				if column == nil {
					return errors.Errorf("failed to lookup cluster column: %v", name)
				}
				splitColumns = append(splitColumns, column)
				clusterColumn = append(clusterColumn, column.Name)
				continue
			}
			clusterColumn = append(clusterColumn, split.ClusterColumns[i])
		}
		tempTable.Clustering = &bigquery.Clustering{
			Fields: clusterColumn,
		}
	}
	j.splitColumns = splitColumns
	if len(splitColumns) == 0 {
		return nil
	}
	schema.Fields = append(schema.Fields, splitColumns...)
	splitRef, _ := base.NewTableReference(j.SplitTable())
	j.Load.DestinationTable = splitRef
	j.SplitSchema = tempTable
	return service.CreateTableIfNotExist(ctx, tempTable, false)
}

func getColumn(fields []*bigquery.TableFieldSchema, column string) *bigquery.TableFieldSchema {
	column = strings.ToLower(column)
	if index := strings.Index(column, "."); index != -1 {
		parent := string(column[:index])
		for i := range fields {
			if parent == strings.ToLower(fields[i].Name) {
				return getColumn(fields[i].Fields, column[index+1:])
			}
		}
	}
	for i := range fields {
		if column == strings.ToLower(fields[i].Name) {
			return fields[i]
		}
	}
	return nil
}
