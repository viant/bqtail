package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBatch_WindowEndTime(t *testing.T) {
	var useCases = []struct {
		description string
		batch       *Batch
		modTime     time.Time
		expect      int
	}{
		{
			description: "less than mid window",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(24, 0),
			expect:      30,
		},
		{
			description: "at the end of the widnow",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(30, 0),
			expect:      40,
		},
		{
			description: "at the beginng of the window",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(21, 0),
			expect:      30,
		},
	}

	for _, useCase := range useCases {
		actual := useCase.batch.WindowEndTime(useCase.modTime)
		actualUnix := actual.Unix()
		assert.EqualValues(t, useCase.expect, actualUnix, useCase.description)
	}

}

func TestBatch_IsWithinFirstHalf(t *testing.T) {
	var useCases = []struct {
		description string
		batch       *Batch
		modTime     time.Time
		expect      bool
	}{
		{
			description: "less than mid window",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(24, 0),
			expect:      true,
		},
		{
			description: "more than mid window",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(26, 0),
			expect:      false,
		},

		{
			description: "at the beginng of the window",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(21, 0),
			expect:      true,
		},
		{
			description: "at the end of the widnow - move you to the beginign of the next",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(30, 0),
			expect:      true,
		},

		{
			description: "before the end of the widnow",
			batch:       NewBatch(10, ""),
			modTime:     time.Unix(29, 0),
			expect:      false,
		},
	}

	for _, useCase := range useCases {
		actual := useCase.batch.IsWithinFirstHalf(useCase.modTime)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}

func TestBatch_WindowURL(t *testing.T) {
	var useCases = []struct {
		description string
		batch       *Batch
		dest        string
		modTime     time.Time
		expect      string
	}{
		{
			description: "less than mid window",
			dest:        "mydata.mytable",
			batch:       NewBatch(10, "mem://127.0.0.1/batch"),
			modTime:     time.Unix(24, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_30.win",
		},
		{
			description: "at the end of the widnow",
			dest:        "mydata.mytable",
			batch:       NewBatch(10, "mem://127.0.0.1/batch"),
			modTime:     time.Unix(30, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_40.win",
		},
		{
			description: "at the beginng of the window",
			dest:        "mydata.mytable",
			batch:       NewBatch(10, "mem://127.0.0.1/batch"),
			modTime:     time.Unix(21, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_30.win",
		},
	}

	for _, useCase := range useCases {
		actual := useCase.batch.WindowURL(useCase.dest, useCase.modTime)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}
