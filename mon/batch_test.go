package mon

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func Test_parseBatch(t *testing.T) {

	var useCases = []struct {
		description string
		input       string
		expect      batch
	}{

		{
			description: "with source hash",
			input:       "xxxx-2015:mydataset.mytable_3529833574555940000_1575579720.win",
			expect:      batch{dest: "mydataset.mytable", dueToRun: time.Unix(1575579720, 0)},
		},

		{
			description: "no source hash",
			input:       "yyyyy:logs_us.xxx_20191205_1575580320.win",
			expect:      batch{dest: "logs_us.xxx_20191205", dueToRun: time.Unix(1575580320, 0)},
		},
	}

	for _, useCase := range useCases {
		actual := parseBatch(useCase.input)
		if !assert.EqualValues(t, &useCase.expect, actual, useCase.description) {
			toolbox.Dump(actual)
		}

	}

}
