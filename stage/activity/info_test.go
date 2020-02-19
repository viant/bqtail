package activity

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestParse(t *testing.T) {

	var useCases = []struct {
		description string
		encoded     string
		expect      interface{}
	}{
		{
			description: "legacy",
			encoded:     "github.com/viant/bqtail--dummy--869694905034386--dispatch",
			expect:      `{"DestTable":"github.com/viant/bqtail--dummy","EventID":"869694905034386","Action":"nop","Mode":"dispatch","Meta":0}`,
		},
		{
			description: "info style",
			encoded:     "github.com/viant/bqtail_dummy--869694905034386_0004_load--dispatch",
			expect:      `{"DestTable":"github.com/viant/bqtail_dummy","EventID":"869694905034386","Action":"load","Mode":"dispatch","Meta":4}`,
		},
		{
			description: "info style location",
			encoded:     "github.com/viant/bqtail:dummy/869694905034386_0004_load/dispatch",
			expect:      `{"DestTable":"github.com/viant/bqtail:dummy","EventID":"869694905034386","Action":"load","Mode":"dispatch","Meta":4}`,
		},
		{
			description: "invalid",
			encoded:     "github.com/viant/bqtail869694905034386--tail",
			expect:      `{"DestTable":"", "Action":"nop","Mode":"tail","Meta":0}`,
		},
		{
			description: "legacy mixed",
			encoded:     "temp--dummy_850558231030311/850558231030311/dispatch",
			expect:      `{"DestTable":"temp--dummy_850558231030311","EventID":"850558231030311","Action":"nop","Mode":"dispatch","Async":true}`,
		},
	}
	for _, useCase := range useCases {
		actual := Parse(useCase.encoded)
		if !assertly.AssertValues(t, useCase.expect, actual, useCase.description) {
			toolbox.Dump(actual)
		}
		actual = Parse(actual.ID())
		assertly.AssertValues(t, useCase.expect, actual, useCase.description)

		//actual = Parse(actual.GetJobID())
		//assertly.AssertValues(t, useCase.expect, actual, useCase.description)
	}

}

func TestInfo_ChildInfo(t *testing.T) {

	var useCases = []struct {
		description string
		encoded     string
		action      string
		step        int
		expect      string
	}{
		{
			description: "top level",
			encoded:     "github.com/viant/bqtail_dummy--869694905034386_00001_query--dispatch",
			action:      "query",
			step:        1,
			expect:      "github.com/viant/bqtail_dummy/869694905034386_01001_query--dispatch",
		},
		{
			description: "leaf level 1",
			action:      "query",
			encoded:     "github.com/viant/bqtail_dummy/869694905034386_01001_query--dispatch",
			expect:      "github.com/viant/bqtail_dummy/869694905034386_02001_query--dispatch",
			step:        1,
		},
		{
			description: "leaf level 2",
			action:      "query",
			encoded:     "github.com/viant/bqtail_dummy/869694905034386_02001_query--dispatch",
			expect:      "github.com/viant/bqtail_dummy/869694905034386_03001_query--dispatch",
			step:        1,
		},
		{
			description: "leaf level 3",
			action:      "query",
			encoded:     "github.com/viant/bqtail_dummy/869694905034386_02002_query--dispatch",
			expect:      "github.com/viant/bqtail_dummy/869694905034386_04003_query--dispatch",
			step:        3,
		},
		{
			description: "query",
			action:      "query",
			encoded:     "875469346223080_00001_query--dispatch",
			expect:      "875469346223080_01001_query--dispatch",
			step:        1,
		},
	}
	for _, useCase := range useCases {
		parent := Parse(useCase.encoded)
		child := parent.ChildInfo(useCase.action, useCase.step)
		assert.Equal(t, useCase.expect, child.ID(), useCase.description)
	}
}
