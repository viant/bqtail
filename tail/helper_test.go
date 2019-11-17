package tail

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWrapRecoverJob(t *testing.T) {

		var useCases  = []struct {
			description string
			jobID string
			expect string
		}{
			{
				description:"regular job",
				jobID:"myjob",
				expect:"recover0001_myjob",
			},
			{
				description:"recver job",
				jobID:"recover0004_myjob",
				expect:"recover0005_myjob",
			},

		}


		for _, useCase := range useCases {
			actual := wrapRecoverJobID(useCase.jobID)
			assert.EqualValues(t, useCase.expect, actual, useCase.description)
		}

}
