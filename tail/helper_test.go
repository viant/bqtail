package tail

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpdateJobId(t *testing.T) {

	var useCases = []struct {
		description string
		jobID       string
		eventID     string
		expect      string
	}{
		{
			description: "event id replcement",
			jobID:       "temp--dummy_850558231030311/850558231030311/dispatch",
			eventID:     "333333",
			expect:      "temp--dummy_333333/333333/dispatch",
		},
		{
			description: "event id replcement",
			jobID:       "temp--dummy_850558231030311_850558231030311/dispatch",
			eventID:     "333333",
			expect:      "333333temp--dummy_850558231030311_850558231030311/dispatch",
		},
	}

	for _, useCase := range useCases {
		updated := updateJobID(useCase.eventID, useCase.jobID)
		assert.EqualValues(t, useCase.expect, updated, useCase.description)
	}

}
