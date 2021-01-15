package bq

import (
	"context"
	"fmt"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Copy copy source to dest table
func (s *service) Copy(ctx context.Context, request *CopyRequest, action *task.Action) (*bigquery.Job, error) {
	if err := request.Init(s.projectID, action); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if request.MultiPartition {
		source := request.sourceTable
		tableId := source.TableId
		if index := strings.Index(tableId, "$"); index != -1 {
			tableId = tableId[:index]
		}

		SQL := s.getPartitionSQL(source, tableId, request)
		isLegacy := strings.Contains(SQL, "[") || strings.Contains(SQL, "]")
		records, err := s.fetchAll(ctx, source.ProjectId, isLegacy, SQL)
		if err != nil {
			return nil, fmt.Errorf("failed to run SQL: %v, %w", SQL, err)
		}
		var job *bigquery.Job
		var onSuccess []*task.Action
		if action != nil {
			onSuccess = action.OnSuccess
			action.OnSuccess = nil
		}
		shared.LogF("[%v] copy.partitions %+v from %v\n", action.Meta.DestTable, records, SQL)
		if len(records) == 0 {
			return nil, nil
		}
		for i, record := range records {
			isLast := i == len(records)-1
			partitionID, ok := record["partition_id"]
			if !ok {
				return nil, fmt.Errorf("failed get parition id  run SQL: %v, record: %v", SQL, record)
			}
			var partitionAction *task.Action
			if action != nil {
				partitionAction = action.Clone()
				partitionAction.Meta.Step += i * 100
			}
			if isLast && partitionAction != nil {
				partitionAction.OnSuccess = onSuccess
			}
			job, err = s.copy(ctx, request.Clone(toolbox.AsString(partitionID)), partitionAction)
			if err != nil {
				return nil, err
			}
		}
		return job, err

	}

	return s.copy(ctx, request, action)
}


func (s *service) getPartitionSQL(source *bigquery.TableReference, tableId string, request *CopyRequest) string {
	if request.PartitionSQL != "" {
		aMap := data.NewMap()
		aMap.SetValue("Source.ProjectID", source.ProjectId)
		aMap.SetValue("Source.DatasetID", source.DatasetId)
		aMap.SetValue("Source.TableID", base.TableID(source.TableId))
		return aMap.ExpandAsText(request.PartitionSQL)
	}
	SQL := fmt.Sprintf("SELECT partition_id FROM [%v:%v.%v$__PARTITIONS_SUMMARY__] WHERE partition_id NOT IN('__NULL__') ORDER BY 1",
		source.ProjectId,
		source.DatasetId,
		tableId)
	fmt.Printf("%v\n", request.PartitionSQL)
	return SQL
}


func (s *service) copy(ctx context.Context, request *CopyRequest, action *task.Action) (*bigquery.Job, error) {
	if request.Template != "" {
		if err := base.RunWithRetries(func() error {
			return s.createFromTemplate(ctx, request.Template, request.destinationTable)
		}); err != nil {
			return nil, err
		}
	}
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Copy: &bigquery.JobConfigurationTableCopy{
				SourceTable:      request.sourceTable,
				DestinationTable: request.destinationTable,
			},
		},
	}

	if request.Append {
		job.Configuration.Copy.WriteDisposition = "WRITE_APPEND"
	} else {
		job.Configuration.Copy.WriteDisposition = "WRITE_TRUNCATE"
	}
	if request.Template == "" {
		job.Configuration.Copy.CreateDisposition = "CREATE_IF_NEEDED"
	}
	s.adjustRegion(ctx, action, job.Configuration.Copy.DestinationTable)
	job.JobReference = action.JobReference()
	if shared.IsInfoLoggingLevel() {
		source := base.EncodeTableReference(job.Configuration.Copy.SourceTable, true)
		dest := base.EncodeTableReference(job.Configuration.Copy.DestinationTable, true)
		shared.LogF("[%v] copy %v into %v\n", action.Meta.DestTable, source, dest)
	}
	return s.Post(ctx, job, action)
}

//CopyRequest represents a copy request
type CopyRequest struct {
	Append           bool
	Source           string
	sourceTable      *bigquery.TableReference
	Dest             string
	destinationTable *bigquery.TableReference
	Template         string
	MultiPartition   bool
	PartitionSQL     string //SQL returning  records with partition_id column
}

//Clone clones copy request with partition
func (r *CopyRequest) Clone(partitionID string) *CopyRequest {
	aMap := data.NewMap()
	aMap.Put(shared.PartitionIDExpr, partitionID)
	aMap.Put(shared.DollarSign, "$")

	return &CopyRequest{
		Append: r.Append,
		Source: aMap.ExpandAsText(r.Source),
		Dest:   aMap.ExpandAsText(r.Dest),
		sourceTable: &bigquery.TableReference{
			ProjectId: r.sourceTable.ProjectId,
			DatasetId: r.sourceTable.DatasetId,
			TableId:   aMap.ExpandAsText(r.sourceTable.TableId),
		},
		destinationTable: &bigquery.TableReference{
			ProjectId: r.destinationTable.ProjectId,
			DatasetId: r.destinationTable.DatasetId,
			TableId:   aMap.ExpandAsText(r.destinationTable.TableId),
		},
		Template:       r.Template,
		MultiPartition: r.MultiPartition,
	}
}

//Init initialises a copy request
func (r *CopyRequest) Init(projectID string, activity *task.Action) (err error) {
	activity.Meta.GetOrSetProject(projectID)
	if r.Source != "" {
		if r.sourceTable, err = base.NewTableReference(r.Source); err != nil {
			return err
		}
	}
	if r.Dest != "" {
		if r.destinationTable, err = base.NewTableReference(r.Dest); err != nil {
			return err
		}
	}
	if r.sourceTable != nil {
		if r.sourceTable.ProjectId == "" {
			r.sourceTable.ProjectId = projectID
		}
	}
	if r.destinationTable != nil {
		if r.destinationTable.ProjectId == "" {
			r.destinationTable.ProjectId = projectID
		}
	}
	return nil
}

//Validate checks if request is valid
func (r *CopyRequest) Validate() error {
	if r.sourceTable == nil {
		return fmt.Errorf("sourceTable was empty")
	}
	if r.destinationTable == nil {
		return fmt.Errorf("destinationTable was empty")
	}
	return nil
}

//NewCopyAction creates a new copy request
func NewCopyAction(source, dest string, append bool, finally *task.Actions) *task.Action {
	copyRequest := &CopyRequest{
		Source: source,
		Dest:   dest,
		Append: append,
	}
	if source != "" {
		copyRequest.sourceTable, _ = base.NewTableReference(source)
	}
	if dest != "" {
		copyRequest.destinationTable, _ = base.NewTableReference(dest)
	}
	result := &task.Action{
		Action:  shared.ActionCopy,
		Actions: finally,
	}
	_ = result.SetRequest(copyRequest)
	return result
}
