package config

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"testing"
	"time"
)

func TestDestination_ExpandTable(t *testing.T) {

	inThePast := time.Unix(1567529212, 326)

	var useCases = []struct {
		description string
		dest        *Destination
		sourceURI   string
		expect      string
		created     time.Time
		hasError    bool
	}{
		{
			description: "mod expression",
			created:     inThePast,
			dest: &Destination{
				Table: "proj:dataset:table_$Mod(4)",
			},
			expect: "proj:dataset:table_3",
		},

		{
			description: "mod invalid expression",
			created:     inThePast,
			dest: &Destination{
				Table: "proj:dataset:table_$Mod(w2",
			},
			hasError: true,
		},
		{
			description: "mod invalid number",
			created:     inThePast,
			dest: &Destination{
				Table: "proj:dataset:table_$Mod(a)",
			},
			hasError: true,
		},
		{
			description: "date expression",
			created:     inThePast.Add(1),
			dest: &Destination{
				Table: "proj:dataset:table_$Mod(7)_$Date",
			},
			expect: "proj:dataset:table_0_20190903",
		},

		{
			description: "sourceURL expression",
			created:     inThePast.Add(1),
			sourceURI:   "gs://bucket/data/2019/02/04/logs_xxx.avro",
			dest: &Destination{
				Table:   "proj:dataset:table_$1$2$3",
				Pattern: "/data/(\\d{4})/(\\d{2})/(\\d{2})/.+",
			},
			expect: "proj:dataset:table_20190204",
		},
		{
			description: "sourceURL invalid expression",
			created:     inThePast.Add(1),
			sourceURI:   "gs://bucket/data/2019/02/04/logs_xxx.avro",
			dest: &Destination{
				Table:   "proj:dataset:table_$1$2$3",
				Pattern: "/data/(\\d{4}/(\\d{2})/(\\d{2})/.+",
			},
			hasError: true,
		},
	}

	for _, useCase := range useCases {
		actual, err := useCase.dest.ExpandTable(useCase.dest.Table, useCase.created, useCase.sourceURI)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}

func TestDestination_TableReference(t *testing.T) {

	inThePast := time.Unix(1567529212, 326)

	var useCases = []struct {
		description string
		dest        *Destination
		sourceURI   string
		expect      *bigquery.TableReference
		created     time.Time
		hasError    bool
	}{
		{
			description: "mod expression",
			created:     inThePast,
			dest: &Destination{
				Table: "proj:dataset.table_$Mod(7)",
			},
			expect: &bigquery.TableReference{
				ProjectId: "proj",
				DatasetId: "dataset",
				TableId:   "table_0",
			},
		},

		{
			description: "mod expression",
			created:     inThePast,
			sourceURI:   "gs://mybucket/data.avro",
			dest: &Destination{
				Table: "dataset.table_$Mod(7)",
			},
			expect: &bigquery.TableReference{
				DatasetId: "dataset",
				TableId:   "table_2",
			},
		},

		{
			description: "invalid table format",
			created:     inThePast,
			dest: &Destination{
				Table: "table",
			},
			hasError: true,
		},
	}

	for _, useCase := range useCases {
		actual, err := useCase.dest.TableReference(useCase.created, useCase.sourceURI)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}
