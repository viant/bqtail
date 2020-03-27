package load

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/sql"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func (j Job) buildTransientActions(actions *task.Actions) (*task.Actions, error) {
	if j.Rule.Dest.Transient == nil {
		return actions, nil
	}
	var result = task.NewActions(nil, nil)
	if actions != nil {
		result.AddOnFailure(actions.OnFailure...)
	}
	tempRef, _ := base.NewTableReference(j.TempTable)
	dropAction := bq.NewDropAction(j.ProjectID, base.EncodeTableReference(tempRef, false))
	actions.AddOnSuccess(dropAction)
	dest := j.Rule.Dest
	load := j.Load

	destinationTable, _ := j.Rule.Dest.CustomTableReference(j.DestTable, j.Source)
	if dest.Schema.Autodetect {
		source := base.EncodeTableReference(load.DestinationTable, false)
		destRef := base.EncodeTableReference(destinationTable, false)
		if j.Rule.IsDMLAppend() && load.Schema != nil {
			j.addAppendDML(load, destinationTable, dest, actions, result)
			return result, nil
		}
		copyRequest := bq.NewCopyAction(source, destRef, j.Rule.IsAppend(), actions)
		result.AddOnSuccess(copyRequest)
		return result, nil
	}

	selectAll := sql.BuildSelect(load.DestinationTable, load.Schema, dest)
	if dest.HasSplit() {
		tempRef, _ := base.NewTableReference(j.TempTable)
		selectAll := sql.BuildSelect(tempRef, load.Schema, dest)
		return result, j.addSplitActions(selectAll, result, actions)
	}
	selectAll = strings.Replace(selectAll, "$WHERE", "", 1)
	partition := base.TablePartition(destinationTable.TableId)
	destTemplate := ""
	if dest.Schema.Template != "" {
		destTemplate = dest.Schema.Template
	}
	if j.Rule.IsDMLAppend() {
		j.addAppendDML(load, destinationTable, dest, actions, result)
		return result, nil
	}

	if len(dest.UniqueColumns) > 0 || partition != "" || len(dest.Transform) > 0 {
		query := bq.NewQueryAction(selectAll, destinationTable, destTemplate, j.Rule.IsAppend(), actions)
		result.AddOnSuccess(query)
	} else {
		source := base.EncodeTableReference(load.DestinationTable, false)
		dest := base.EncodeTableReference(destinationTable, false)
		copyRequest := bq.NewCopyAction(source, dest, j.Rule.IsAppend(), actions)
		result.AddOnSuccess(copyRequest)
	}
	return result, nil
}

func (j Job) addAppendDML(load *bigquery.JobConfigurationLoad, destinationTable *bigquery.TableReference, dest *config.Destination, actions *task.Actions, result *task.Actions) {
	SQL := sql.BuilAppendDML(load.DestinationTable, destinationTable, load.Schema, dest)
	SQL = strings.Replace(SQL, "$WHERE", "", 1)
	query := bq.NewQueryAction(SQL, nil, "", true, actions)
	result.AddOnSuccess(query)
}
