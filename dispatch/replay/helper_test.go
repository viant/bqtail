package replay

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJobID(t *testing.T) {


		var useCases  = []struct {
			description string
			baseURL string
			URL string
			expect string
		}{
			{
				baseURL:"gs://viant_e2e_bqdispatch/BqDispatch/Tasks",
				URL:"gs://viant_e2e_bqdispatch/BqDispatch/Tasks/temp/nobid_9_20191112_835297276185503/835297276185503/dispatch.json",
				expect:"temp--nobid_9_20191112_835297276185503--835297276185503--dispatch",
			},
		}


		for _, useCase := range useCases {
			jobID := JobID(useCase.baseURL, useCase.URL)
			assert.EqualValues(t, useCase.expect, jobID, useCase.description)
		}


}
