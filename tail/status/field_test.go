package status

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"log"
	"testing"
)

func TestField_AdjustType(t *testing.T) {

	var useCases = []struct {
		description string
		field       *Field
		expectType  string
		location    string
		assets      []*asset.Resource
		hasError    bool
	}{
		{
			description: "string type",
			location:    "mem://localhost/status/field/case001",
			field: &Field{
				Name:     "name",
				Location: "mem://localhost/status/field/case001/data.json",
				Row:      1,
				Type:     "",
			},
			expectType: "STRING",
			assets: []*asset.Resource{
				{
					Name: "data.json",
					Data: []byte(`{"id": 101, "name": "dummy 1", "type_id": 1, "billable":  true}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}`),
				},
			},
		},
		{
			description: "boolean type",
			location:    "mem://localhost/status/field/case002",
			field: &Field{
				Name:     "billable",
				Location: "mem://localhost/status/field/case002/data.json",
				Row:      1,
				Type:     "",
			},
			expectType: "BOOLEAN",
			assets: []*asset.Resource{
				{
					Name: "data.json",
					Data: []byte(`{"id": 101, "name": "dummy 1", "type_id": 1, "billable":  true}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}`),
				},
			},
		},
		{
			description: "float type",
			location:    "mem://localhost/status/field/case003",
			field: &Field{
				Name:     "value",
				Location: "mem://localhost/status/field/case003/data.json",
				Row:      1,
				Type:     "",
			},
			expectType: "FLOAT64",
			assets: []*asset.Resource{
				{
					Name: "data.json",
					Data: []byte(`{"id": 101, "name": "dummy 1", "type_id": 1, "value": 3.4}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}`),
				},
			},
		},
		{
			description: "int type",
			location:    "mem://localhost/status/field/case004",
			field: &Field{
				Name:     "value",
				Location: "mem://localhost/status/field/case004/data.json",
				Row:      1,
				Type:     "",
			},
			expectType: "INT64",
			assets: []*asset.Resource{
				{
					Name: "data.json",
					Data: []byte(`{"id": 101, "name": "dummy 1", "type_id": 1, "value": 3}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}`),
				},
			},
		},

		{
			description: "unsupported yey type",
			location:    "mem://localhost/status/field/case004",
			field: &Field{
				Name:     "value",
				Location: "mem://localhost/status/field/case004/data.json",
				Row:      1,
				Type:     "",
			},
			hasError: true,
			assets: []*asset.Resource{
				{
					Name: "data.json",
					Data: []byte(`{"id": 101, "name": "dummy 1", "type_id": 1, "value": [1,2,3]}
{"id": 102, "name": "dummy 2", "type_id": 1}
{"id": 103, "name": "dummy 3", "type_id": 1}`),
				},
			},
		},
	}

	ctx := context.Background()
	for _, useCase := range useCases {
		fs := afs.New()
		mgr, err := afs.Manager(useCase.location)
		if err != nil {
			log.Fatal(err)
		}
		err = asset.Create(mgr, useCase.location, useCase.assets)
		if err != nil {
			log.Fatal(err)
		}

		err = useCase.field.AdjustType(ctx, fs)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		assert.Equal(t, useCase.expectType, useCase.field.Type, useCase.description)

	}

}
