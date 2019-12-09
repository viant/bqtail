package mon

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_parseLoad(t *testing.T) {

	now := time.Now()
	var useCases = []struct {
		description string
		URL         string
		baseURL     string
		expect      *load
	}{
		{
			description: "run URL with lead location",
			URL:         "gs://xx_operation/BqTail/Journal/Running/mydataset.mytable/440355876564413.run",
			baseURL:     "gs://xx_operation/BqTail/Journal",
			expect:      &load{dest: "mydataset.mytable", eventID: "440355876564413", started: now},
		},
		{
			description: "run URL with flat location",
			URL:         "gs://xx_operation/BqTail/Journal/Running/mydataset.mytable2--440355876564414.run",
			baseURL:     "gs://xx_operation/BqTail/Journal/",
			expect:      &load{dest: "mydataset.mytable2", eventID: "440355876564414", started: now},
		},
	}

	for _, useCase := range useCases {
		actual := parseLoad(useCase.baseURL, useCase.URL, now)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}
