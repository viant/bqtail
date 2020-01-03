package batch

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_isPreConditionError(t *testing.T) {
	var useCases = []struct {
		description string
		input       error
		expect      bool
	}{
		{
			description: "precondition error",
			input:       errors.New("failed to acquire batch window: failed to upload: gs://viant_e2e_bqdispatch/BqDispatch/Tasks/bqtail.dummy_v24_1113913261899497184_1577747790.win: googleapi: Error 412: Precondition Failed,"),
			expect:      true,
		},

		{
			description: "generic error",
			input:       errors.New("failed to acquire batch window: failed to upload: gs://viant_e2e_bqdispatch/BqDispatch/Tasks/bqtail.dummy_v24_1113913261899497184_1577747790.win"),
			expect:      false,
		},
		{
			description: "nil error",
			input:       nil,
			expect:      false,
		},
	}

	for _, useCase := range useCases {
		actual := isPreConditionError(useCase.input)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}
