package dispatch

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_URLToWindowEndTime(t *testing.T) {

	var useCases = []struct {
		description string
		URL         string
		hasError    bool
		expect      int
	}{
		{
			description: "valid URL",
			URL:         "mem://127.0.0.1/batch/mydata.mytable_40.win",
			expect:      40,
		},

		{
			description: "invalid URL",
			URL:         "mem://127.0.0.1/batch/mydata.mytable40.win",
			hasError:    true,
		},
	}

	for _, useCase := range useCases {

		endTime, err := URLToWindowEndTime(useCase.URL)
		if useCase.hasError {
			assert.NotNil(t, useCase.URL, useCase.description)
			continue
		}

		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		actual := endTime.Unix()
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}
