package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRules_Match(t *testing.T) {

	var useCases = []struct {
		description     string
		sourceURL       string
		rules           *Rules
		expectRuleIndex int
		hasError        bool
	}{

		{
			description: "match by ext",
			rules: &Rules{
				Items: []*Rule{
					{
						Source: Resource{
							Ext: ".csv",
						},
					},
				},
			},
			sourceURL:       "gs://abc/data/file.csv",
			expectRuleIndex: 0,
		},
		{
			description: "no match by ext",
			rules: &Rules{
				Items: []*Rule{
					{
						Source: Resource{
							Ext: ".csv",
						},
					},
				},
			},
			expectRuleIndex: -1,
			sourceURL:       "gs://abc/data/file.csv.gz",
		},

		{
			description: "match by prefix and ext",
			rules: &Rules{
				Items: []*Rule{
					{
						Source: Resource{
							Prefix: "/data/",
							Ext:    ".csv",
						},
					},
				},
			},
			sourceURL:       "gs://abc/data/file.csv",
			expectRuleIndex: 0,
		},

		{
			description: "no match by prefix and ext",
			rules: &Rules{
				Items: []*Rule{
					{
						Source: Resource{
							Prefix: "/data2/",
							Ext:    ".csv",
						},
					},
				},
			},
			sourceURL:       "gs://abc/data/file.csv",
			expectRuleIndex: -1,
		},

		{
			description: "invalid URL",
			rules: &Rules{
				Items: []*Rule{
					{
						Source: Resource{
							Prefix: "/data2/",
							Ext:    ".csv",
						},
					},
				},
			},
			hasError:        true,
			sourceURL:       "://abc/data/file.csv",
			expectRuleIndex: -1,
		},
	}

	for _, useCase := range useCases {
		actual, err := useCase.rules.Match(useCase.sourceURL)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		if useCase.expectRuleIndex == -1 {
			assert.Nil(t, actual, useCase.description)
			continue
		}
		expect := useCase.rules.Items[useCase.expectRuleIndex]
		assert.EqualValues(t, expect, actual, useCase.description)
	}

}
