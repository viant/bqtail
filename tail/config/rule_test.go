package config

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/matcher"
	"testing"
)

func TestRoute_HasMatch(t *testing.T) {

	var useCases = []struct {
		description string
		Rule
		URL    string
		expect bool
	}{
		{
			description: "prefix match",
			Rule: Rule{
				When: matcher.Basic{
					Prefix: "/folder/",
				},
				Dest: &Destination{
					Table: "project:dataset:table1",
				},
			},
			URL:    "ssh:///folder/abc.xom",
			expect: true,
		},
		{
			description: "prefix no match",
			Rule: Rule{
				When: matcher.Basic{
					Prefix: "folder/",
				},
				Dest: &Destination{
					Table: "project:dataset:table2",
				},
			},
			URL:    "ssh:///f/abc.xom",
			expect: false,
		},
		{
			description: "suffix match",
			Rule: Rule{
				When: matcher.Basic{
					Suffix: ".csv",
				},
				Dest: &Destination{
					Table: "project:dataset:table3",
				},
			},
			URL:    "ssh:///folder/abc.csv",
			expect: true,
		},
		{
			description: "suffix no match",
			Rule: Rule{
				When: matcher.Basic{
					Suffix: ".tsv",
				},
				Dest: &Destination{
					Table: "project:dataset:table4",
				},
			},
			URL:    "ssh:///f/abc.ts",
			expect: false,
		},
		{
			description: "filter no match",
			Rule: Rule{
				When: matcher.Basic{
					Suffix: ".tsv",
					Filter: `^[a-z]*/data/\\d+/`,
				},
				Dest: &Destination{
					Table: "project:dataset:table5",
				},
			},
			URL:    "ssh://host/123/abc.tsv",
			expect: false,
		},
		{
			description: "filter match",
			Rule: Rule{
				When: matcher.Basic{
					Suffix: ".tsv",
					Filter: `^\/[a-z]+/data/\d+/`,
				},
				Dest: &Destination{
					Table: "project:dataset:table6",
				},
			},
			URL:    "ssh://host/aa/data/002/abc.tsv",
			expect: true,
		},
		{
			description: "filter -  match",
			Rule: Rule{
				When: matcher.Basic{
					Prefix: "/gcs-logging/",
				},
				Dest: &Destination{
					Table: "project:dataset:table6",
				},
			},
			URL:    "gs://viant_dataflow_bqtail/gcs-logging/PROJECT_viant-adelphic_BUCKET_adelphic-keys_usage_2019_10_23_22_00_00_00a90fd57818ed9df3_v0",
			expect: true,
		},
	}

	for _, useCase := range useCases {
		actual := useCase.HasMatch(useCase.URL)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}
