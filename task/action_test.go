package task

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/bqtail/stage"
	"github.com/viant/toolbox/data"
	"testing"
)

func TestActions_Expand(t *testing.T) {

	useCases := []struct {
		description string
		action      Action
		expanded    Action
		req         map[string]interface{}
		expander    map[string]interface{}
		root        *stage.Process
	}{
		{
			description: "url expansion",
			action: Action{
				Action: "delete",
				Request: map[string]interface{}{
					"URLs": "$LoadURIs",
				},
			},
			expander: map[string]interface{}{
				"LoadURIs": "gs://viant_e2e_bqtail/data/case002/dummy.json",
			},

			expanded: Action{
				Action: "delete",
				Request: map[string]interface{}{
					"URLs": "gs://viant_e2e_bqtail/data/case002/dummy.json",
				},
			},
		},
	}

	for _, useCase := range useCases {
		expander := data.Map(useCase.expander)
		expanded := useCase.action.Expand(useCase.req, useCase.root, expander)
		assert.EqualValues(t, useCase.expanded, *expanded, useCase.description)
	}

}
