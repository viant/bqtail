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
			batch:       NewBatch(10),
			modTime:     time.Unix(24, 0),
			expect:      30,
		},
		{
			description: "at the end of the widnow",
			batch:       NewBatch(10),
			modTime:     time.Unix(30, 0),
			expect:      40,
		},
		{
			description: "at the beginng of the window",
			batch:       NewBatch(10),
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
			batch:       NewBatch(10),
			modTime:     time.Unix(24, 0),
			expect:      true,
		},
		{
			description: "more than mid window",
			batch:       NewBatch(10),
			modTime:     time.Unix(26, 0),
			expect:      false,
		},

		{
			description: "at the beginng of the window",
			batch:       NewBatch(10),
			modTime:     time.Unix(21, 0),
			expect:      true,
		},
		{
			description: "at the end of the widnow - move you to the beginign of the next",
			batch:       NewBatch(10),
			modTime:     time.Unix(30, 0),
			expect:      true,
		},

		{
			description: "before the end of the widnow",
			batch:       NewBatch(10),
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
			batch:       NewBatch(10),
			modTime:     time.Unix(24, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_30.win",
		},
		{
			description: "at the end of the widnow",
			dest:        "mydata.mytable",
			batch:       NewBatch(10),
			modTime:     time.Unix(30, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_40.win",
		},
		{
			description: "at the beginng of the window",
			dest:        "mydata.mytable",
			batch:       NewBatch(10),
			modTime:     time.Unix(21, 0),
			expect:      "mem://127.0.0.1/batch/mydata.mytable_30.win",
		},
	}

	for _, useCase := range useCases {
		actual := useCase.batch.WindowURL("mem://127.0.0.1/batch", useCase.dest, useCase.modTime)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)

	}

}

func TestBatch_GroupKey(t *testing.T) {
	batch := &Batch{GroupByParameters: []string{"UidType", "UserPoolID"}}

	ip123, err := batch.GroupKey(map[string]interface{}{"UidType": "ip", "UserPoolID": "123", "Ignored": "a"})
	assert.NoError(t, err)
	ip123Again, err := batch.GroupKey(map[string]interface{}{"UidType": "ip", "UserPoolID": "123", "Ignored": "b"})
	assert.NoError(t, err)
	device123, err := batch.GroupKey(map[string]interface{}{"UidType": "device", "UserPoolID": "123"})
	assert.NoError(t, err)
	ip234, err := batch.GroupKey(map[string]interface{}{"UidType": "ip", "UserPoolID": "234"})
	assert.NoError(t, err)

	assert.Equal(t, ip123, ip123Again)
	assert.NotEqual(t, ip123, device123)
	assert.NotEqual(t, ip123, ip234)
	assert.Len(t, ip123, 64)
}

func TestBatch_GroupKeyDisabled(t *testing.T) {
	key, err := (&Batch{}).GroupKey(nil)
	assert.NoError(t, err)
	assert.Empty(t, key)
}

func TestBatch_GroupKeyMissingParameter(t *testing.T) {
	batch := &Batch{GroupByParameters: []string{"UidType", "UserPoolID"}}
	_, err := batch.GroupKey(map[string]interface{}{"UidType": "ip"})
	assert.EqualError(t, err, `batch grouping parameter "UserPoolID" was not found`)
}
