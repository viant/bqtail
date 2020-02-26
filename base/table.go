package base

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//NewTableReference creates a table reference for table in the following syntax [project:]dataset.table
func NewTableReference(table string) (*bigquery.TableReference, error) {
	dotIndex := strings.LastIndex(table, ".")
	if dotIndex == -1 {
		return nil, fmt.Errorf("datasetID is missing, expected [ProjectID:].DatasetID.%v", table)
	}
	count := strings.Count(table, ".")
	if count == 2 {
		table = strings.Replace(table, ".", ":", 1)
	}
	tableID := string(table[dotIndex+1:])
	datasetID := string(table[:dotIndex])
	projectID := ""
	if index := strings.Index(datasetID, ":"); index != -1 {
		projectID = string(datasetID[:index])
		datasetID = string(datasetID[index+1:])
	}
	return &bigquery.TableReference{
		TableId:   tableID,
		DatasetId: datasetID,
		ProjectId: projectID,
	}, nil
}

//EncodeTableReference encodes table reference
func EncodeTableReference(table *bigquery.TableReference, standardSQL bool) string {
	if table.ProjectId == "" {
		return fmt.Sprintf("%v.%v", table.DatasetId, table.TableId)
	}
	projectSeparator := ":"
	if standardSQL {
		projectSeparator = "."
	}
	return table.ProjectId + projectSeparator + table.DatasetId + "." + table.TableId
}

//TableID returns a table id
func TableID(table string) string {
	tableID := table
	if index := strings.Index(tableID, "$"); index != -1 {
		return string(tableID[:index])
	}
	return tableID
}

//TablePartition returns a table partition
func TablePartition(table string) string {
	tableID := table
	if index := strings.Index(tableID, "$"); index != -1 {
		return string(tableID[index+1:])
	}
	return ""
}
