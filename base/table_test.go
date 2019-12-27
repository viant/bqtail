package base

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"testing"
)

func TestEncodeTableReference(t *testing.T) {

	var useCases = []struct {
		description string
		input       string
		expect      *bigquery.TableReference
		hasError    bool
	}{
		{
			description: "legact table format",
			input:       "project:dataset.table",
			expect: &bigquery.TableReference{
				DatasetId: "dataset",
				ProjectId: "project",
				TableId:   "table",
			},
		},
		{
			description: "standard table format",
			input:       "project.dataset.table",
			expect: &bigquery.TableReference{
				DatasetId: "dataset",
				ProjectId: "project",
				TableId:   "table",
			},
		},
		{
			description: "dataset and table",
			input:       "dataset.table",
			expect: &bigquery.TableReference{
				DatasetId: "dataset",
				TableId:   "table",
			},
		},
		{
			description: "invalid table",
			input:       "table",
			hasError:    true,
		},
	}

	for _, useCase := range useCases {
		table, err := NewTableReference(useCase.input)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		if !assert.EqualValues(t, useCase.expect, table, useCase.description) {
			_ = toolbox.DumpIndent(table, true)
		}

	}

}
