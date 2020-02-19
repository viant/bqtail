package config

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs/matcher"
	"testing"
)

func TestRoutes_MatchByTable(t *testing.T) {
	var useCases = []struct {
		description string
		Ruleset     []*Rule
		table       string
		expextTable string
	}{
		{
			description: "dataset and table match",
			Ruleset: []*Rule{
				{
					Dest: &Destination{
						Table: "project:dataset.table1",
					},
				},
				{
					Dest: &Destination{
						Table: "dataset.table2",
					},
				},
			},
			table:       "dataset.table1",
			expextTable: "project:dataset.table1",
		},
		{
			description: "project, dataset and table match",
			Ruleset: []*Rule{
				{
					Dest: &Destination{
						Table: "project:dataset.table1",
					},
				},
				{
					Dest: &Destination{
						Table: "project:dataset.table2",
					},
				},
			},
			table:       "dataset.table1",
			expextTable: "project:dataset.table1",
		},
		{
			description: "partition table match",
			Ruleset: []*Rule{
				{
					Dest: &Destination{
						Table: "project:dataset.table1_$Mod(15)",
					},
				},
				{
					Dest: &Destination{
						Table: "project:dataset.table2",
					},
				},
			},
			table:       "dataset.table1_12",
			expextTable: "project:dataset.table1_$Mod(15)",
		},
	}

	for _, useCase := range useCases {
		rules := &Ruleset{
			Rules: useCase.Ruleset,
		}
		rule := rules.MatchByTable(useCase.table)
		if useCase.expextTable != "" {
			if !assert.NotNil(t, rule, useCase.description) {
				continue
			}
			assert.Equal(t, useCase.expextTable, rule.Dest.Table, useCase.description)
		}
	}

}

func TestRoutes_HasMatch(t *testing.T) {
	var useCases = []struct {
		description string
		Ruleset     []*Rule
		URL         string
		expextTable string
	}{
		//{
		//	description: "suffix match",
		//	Ruleset: []*Rule{
		//		{
		//			When: matcher.Basic{
		//				Mode: ".tsv",
		//			},
		//			Dest: &Destination{
		//				Table: "project:dataset:table1",
		//			},
		//		},
		//		{
		//			When: matcher.Basic{
		//				Mode: ".csv",
		//			},
		//			Dest: &Destination{
		//				Table: "project:dataset:table2",
		//			},
		//		},
		//	},
		//
		//	SourceURL:         "ssh://zz/folder/a.csv",
		//	expextTable: "project:dataset:table2",
		//},
		//{
		//	description: "prefix with long file",
		//	Ruleset: []*Rule{
		//		{
		//			When: matcher.Basic{
		//				Prefix: "/s",
		//			},
		//			Dest: &Destination{
		//				Table: "project:dataset:table3",
		//			},
		//		},
		//		{
		//			When: matcher.Basic{
		//				Prefix: "/g",
		//			},
		//			Dest: &Destination{
		//				Table: "project:dataset:table4",
		//			},
		//		},
		//	},
		//
		//	SourceURL:         "ssh://zz/folder/a.csv",
		//	expextTable: "",
		//},
		{
			description: "pattern match",
			Ruleset: []*Rule{
				{
					When: matcher.Basic{
						Filter: "/data/case028/(\\d{4})/(\\d{2})/(\\d{2})/.+",
					},
					Dest: &Destination{
						Table: "project:dataset:table4",
					},
				},
			},
			expextTable: "project:dataset:table4",
			URL:         "gs://viant_e2e_bqtail/data/case028/2020/01/01/dummy_5132008211853229192_0_0001.json",
		},
	}

	for _, useCase := range useCases {
		rules := &Ruleset{
			Rules: useCase.Ruleset,
		}

		actual := rules.Match(useCase.URL)
		if useCase.expextTable == "" {
			assert.True(t, len(actual) == 0, useCase.description)
			continue
		}

		if !assert.True(t, len(actual) > 0, useCase.description) {
			continue
		}

		assert.Equal(t, useCase.expextTable, actual[0].Dest.Table, useCase.description)
	}
}
