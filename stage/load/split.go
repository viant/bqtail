package load

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/bqtail/service/bq"
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
	if next == nil {
		next = task.NewActions(nil, nil)
	}
	dest := j.Rule.Dest
	for i := range split.Mapping {
		mapping := split.Mapping[i]
		destTable, _ := dest.CustomTableReference(mapping.Then, j.Process.Source)
		SQL := strings.Replace(selectSQL, "$WHERE", " WHERE  "+mapping.When+" ", 1)
		query := bq.NewQueryAction(SQL, destTable, j.Rule.IsAppend(), next)
		group := task.NewActions(nil, nil)
		group.AddOnSuccess(query)
		next = group
	}

	if len(split.ClusterColumns) > 0 {
		setColumns := []string{}
		for i, column := range split.ClusterColumns {
			if index := strings.LastIndex(split.ClusterColumns[i], "."); index != -1 {
				setColumns = append(setColumns, fmt.Sprintf("%v = %v ", string(column[index+1:]), column))
			}
		}
		if len(setColumns) > 0 {
			DML := fmt.Sprintf("UPDATE %v SET %v WHERE 1=1", j.TempTable, strings.Join(setColumns, ","))
			query := bq.NewQueryAction(DML, nil, j.Rule.IsAppend(), next)
			result.AddOnSuccess(query)
		}
	} else {
		result.AddOnSuccess(next.OnSuccess...)
		result.AddOnSuccess(next.OnFailure...)
	}
	return nil
}



func (j *Job) applySplitSchemaOptimization() error {
	split := j.Rule.Dest.Schema.Split
	if j.Load.Schema == nil {
		return nil
	}
	if len(split.ClusterColumns) > 0 {
		if split.TimeColumn == "" {
			split.TimeColumn = "ts"
		}
		field := getColumn(j.Load.Schema.Fields, split.TimeColumn)
		if field == nil {
			j.Load.Schema.Fields = append(j.Load.Schema.Fields, &bigquery.TableFieldSchema{
				Name: split.TimeColumn,
				Type: timestampDataType,
			})
		}
		j.Load.TimePartitioning = &bigquery.TimePartitioning{
			Field: split.TimeColumn,
			Type:  timePartitionType,
		}
		var clusterdColumn = make([]string, 0)
		for i, name := range split.ClusterColumns {
			if strings.Contains(split.ClusterColumns[i], ".") {
				column := getColumn(j.Load.Schema.Fields, split.ClusterColumns[i])
				if column == nil {
					return errors.Errorf("failed to lookup cluster column: %v", name)
				}
				j.Load.Schema.Fields = append(j.Load.Schema.Fields, column)
				clusterdColumn = append(clusterdColumn, column.Name)
				continue
			}
			clusterdColumn = append(clusterdColumn, split.ClusterColumns[i])
		}

		j.Load.Clustering = &bigquery.Clustering{
			Fields: clusterdColumn,
		}
	}
	return nil
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
