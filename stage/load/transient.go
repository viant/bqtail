package load

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/schema"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/sql"
	"github.com/viant/bqtail/task"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

func (j Job) buildTransientActions(actions *task.Actions) (*task.Actions, error) {
	if j.Rule.Dest.Transient == nil || j.DestTable == "" {
		return actions, nil
	}
	var result = task.NewActions(nil, nil)
	if actions != nil {
		result.AddOnFailure(actions.OnFailure...)
	}
	tempRef, _ := base.NewTableReference(j.TempTable)
	dropAction := bq.NewDropAction(j.ProjectID, base.EncodeTableReference(tempRef, false))
	actions.FinalizeOnSuccess(dropAction)
	dest := j.Rule.Dest.Clone()
	load := j.Load

	destinationTable, _ := dest.CustomTableReference(j.DestTable, j.Source)

	if dest.Schema.Autodetect {
		source := base.EncodeTableReference(load.DestinationTable, false)
		destRef := base.EncodeTableReference(destinationTable, false)
		if j.Rule.IsDMLCopy() && load.Schema != nil {
			j.addDMLCopy(load, destinationTable, dest, actions, result)
			return result, nil
		}
		copyRequest := bq.NewCopyAction(source, destRef, j.Rule.IsAppend(), actions)
		result.AddOnSuccess(copyRequest)
		return result, nil
	}

	selectAll := sql.BuildSelect(load.DestinationTable, load.Schema, dest, j.getDestTableSchema())
	if dest.HasSplit() {
		tempRef, _ := base.NewTableReference(j.TempTable)
		selectAll := sql.BuildSelect(tempRef, load.Schema, dest, j.getDestTableSchema())
		return result, j.addSplitActions(selectAll, result, actions)
	}

	selectAll = strings.Replace(selectAll, "$WHERE", j.getDMLWhereClause(), 1)
	partition := base.TablePartition(destinationTable.TableId)
	destTemplate := ""

	if dest.Schema.Template != "" {
		destTemplate = dest.Schema.Template
	}


	if j.Rule.IsDMLCopy() {
		j.addDMLCopy(load, destinationTable, dest, actions, result)
		return result, nil
	}
	canCopy := schema.CanCopy(j.TempSchema, j.DestSchema)

	if j.Rule.Dest.IsCopyMethodQuery() || partition != "" || !canCopy {
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

func (j Job) addDMLCopy(load *bigquery.JobConfigurationLoad, destinationTable *bigquery.TableReference, dest *config.Destination, actions *task.Actions, result *task.Actions) {
	SQL := sql.BuildAppendDML(load.DestinationTable, destinationTable, load.Schema, dest, j.getDestTableSchema())
	SQL = strings.Replace(SQL, "$WHERE", j.getDMLWhereClause(), 1)
	query := bq.NewDMLAction(SQL, destinationTable, j.TempTable, true, actions)
	result.AddOnSuccess(query)
}

func (j Job) getDMLWhereClause() string {
	where := ""
	if j.Rule.Dest.Transient.Criteria != "" {
		where = " WHERE " + j.Rule.Dest.Transient.Criteria
	}
	return where
}
