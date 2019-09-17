package config

import (
	"bqtail/dispatch/contract"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"testing"
)

func TestEvent_Match(t *testing.T) {

	var useCases = []struct {
		description string
		filter      *Filter
		event       *contract.Job
		hasError    bool
		expect      bool
	}{
		{
			description: "filter by source dataset match",
			expect:      true,
			filter: &Filter{
				Source: ".+:myDataset\\..+",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "COPY",
					Copy: &bigquery.JobConfigurationTableCopy{
						SourceTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "sourceTable",
						},
						DestinationTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "myDestTable",
						},
					},
				},
			},
		},
		{
			description: "filter by target table does match",
			expect:      false,
			filter: &Filter{
				Dest: ".+myDestTable2",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "LOAD",
					Load: &bigquery.JobConfigurationLoad{
						DestinationTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "myDestTable1",
						},
					},
				},
			},
		},

		{
			description: "filter by job type and URI match",
			expect:      true,
			filter: &Filter{
				Type: "EXTRACT",
				Dest: ".+table10",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "EXTRACT",
					Extract: &bigquery.JobConfigurationExtract{
						SourceTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "mySourceTable",
						},
						DestinationUri: "gs://myBucket/table10",
					},
				},
			},
		},
		{
			description: "filter by job type and URI does not match",
			expect:      false,
			filter: &Filter{
				Type: "EXTRACT",
				Dest: ".+table2",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "EXTRACT",
					Extract: &bigquery.JobConfigurationExtract{
						SourceTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "mySourceTable",
						},
						DestinationUri: "gs://myBucket/table10",
					},
				},
			},
		},

		{
			description: "filter by source match",
			expect:      true,
			filter: &Filter{
				Type:   "QUERY",
				Source: ".+tableX",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "QUERY",
					Query: &bigquery.JobConfigurationQuery{
						DestinationTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "mySourceTable",
						},
						Query: "SELECT  FROM tableX WHERE 1 = 0",
					},
				},
			},
		},

		{
			description: "filter by source does not match",
			expect:      false,
			filter: &Filter{
				Type:   "QUERY",
				Source: ".+tableX.*",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "QUERY",
					Query: &bigquery.JobConfigurationQuery{
						DestinationTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "mySourceTable",
						},
						Query: "SELECT  FROM tableY",
					},
				},
			},
		},
		{
			description: "filter by dest  match",
			expect:      true,
			filter: &Filter{
				Type: "QUERY",
				Dest: ".+myDataset\\.myDestTable",
			},
			event: &contract.Job{
				Configuration: &bigquery.JobConfiguration{
					JobType: "QUERY",
					Query: &bigquery.JobConfigurationQuery{
						DestinationTable: &bigquery.TableReference{
							ProjectId: "myProject",
							DatasetId: "myDataset",
							TableId:   "myDestTable",
						},
						Query: "SELECT  FROM tableY",
					},
				},
			},
		},
	}

	for _, useCase := range useCases {
		err := useCase.filter.Init()
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actual := useCase.filter.Match(useCase.event)
		assert.Equal(t, useCase.expect, actual, useCase.description)
	}

}
