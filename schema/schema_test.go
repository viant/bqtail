package schema

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestNew(t *testing.T) {

	useCases := []struct {
		description string
		data        string
		expect interface{}
	}{
		{
			description:"basic record",
			data:`{
	"k1":"test",
	"k2": true,
	"k3": 1,
    "k4": 3.2,
    "k5": null
}`,

		expect:`[
    {"@indexBy@":"name"},
	{
		"name": "k1",
		"type": "STRING"
	},
	{
		"name": "k2",
		"type": "BOOLEAN"
	},
	{
		"name": "k3",
		"type": "INT64"
	},
	{
		"name": "k4",
		"type": "INT64"
	},
	{
		"name": "k5",
		"type": "STRING"
	}
]`,
		},

		{
			description: "basic record",
			data: `{
  "k1": "test",
  "k2": [1,2,3],
  "k3": ["1","2"],
  "k4": 3.2,
  "k5": {
    "k6": 1,
    "k7": true
  },
  "k8": [
    {
      "i1": 1
    },
    {
      "i2": "test"
    },
    {
      "i2": "test2",
      "i3": true
    }
  ]
}`,

			expect: `[
	{"@indexBy@":"name"},
	{
		"name": "k4",
		"type": "INT64"
	},
	{
		"fields": [
			{"@indexBy@":"name"},
			{
				"name": "k6",
				"type": "INT64"
			},
			{
				"name": "k7",
				"type": "BOOLEAN"
			}
		],
		"name": "k5",
		"type": "RECORD"
	},
	{
		"fields": [
			{"@indexBy@":"name"},
			{
				"name": "i1",
				"type": "INT64"
			},
			{
				"name": "i2",
				"type": "STRING"
			},
			{
				"name": "i3",
				"type": "BOOLEAN"
			}
		],
		"mode": "REPEATED",
		"name": "k8",
		"type": "RECORD"
	},
	{
		"name": "k1",
		"type": "STRING"
	},
	{
		"mode": "REPEATED",
		"name": "k2",
		"type": "INT64"
	},
	{
		"mode": "REPEATED",
		"name": "k3",
		"type": "STRING"
	}
]`,
		},
	}

	for _, useCase := range useCases {
		data := map[string]interface{}{}
		err := json.Unmarshal([]byte(useCase.data), &data)
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		schema, err := New(data, "")
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		actual, _ := json.Marshal(schema)
		if ! assertly.AssertValues(t, useCase.expect, string(actual), useCase.description) {
			toolbox.DumpIndent(schema, true)
		}
	}

}
