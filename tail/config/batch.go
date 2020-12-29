package config

import (
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/task"
	"time"
)

//Batch transfer config
type (
	Batch struct {
		//Window batch time window
		Window *Window `json:",omitempty"`

		//RollOver if this flag is set, if the first event of the batch fall outside of the first half time, the window can be expanded if previous window had not existed.
		//Do not use on prod, it was intended for testing only
		RollOver bool `json:",omitempty"`

		//MultiPath is one batch can collect files from various folder
		MultiPath bool `json:",omitempty"`

		//MaxDelayInSec delay before collecting batch file. to randomly distribute workload,
		// when a table has 40 shards, 40 batches would start exactly at the same time unless this parameter is specified
		MaxDelayInSec int `json:",omitempty"`

		//Group batch grouping setting
		Group *Group
	}

	//Group represent batch group
	Group struct {
		OnDone []*task.Action
	}
)

//Init initialises batch mode
func (b *Batch) Init() {
	if b.Window == nil {
		b.Window = &Window{}
	}
	b.Window.Init()

}

//MaxDelayMs max delay in ms
func (b *Batch) MaxDelayMs(minInMs int) int {
	maxDelayMs := b.MaxDelayInSec * 1000
	if maxDelayMs < minInMs {
		return minInMs + 1000
	}
	return maxDelayMs
}

//Validate checks if batch configuration is valid
func (b *Batch) Validate() error {
	return b.Window.Validate()
}

//IsWithinFirstHalf returns true if source time is within the first half window
func (b *Batch) IsWithinFirstHalf(sourceTime time.Time) bool {
	halfDuration := b.Window.DurationInSec / 2
	remainder := int(sourceTime.Unix()) % b.Window.DurationInSec
	return remainder < halfDuration
}

//WindowEndTime returns window end time
func (b *Batch) WindowEndTime(sourceTime time.Time) time.Time {
	windowDuration := b.Window.DurationInSec
	sourceUnixTimestamp := sourceTime.Unix()
	remainder := int(sourceUnixTimestamp) % windowDuration
	endTimeWindowDelta := windowDuration - remainder
	return time.Unix(sourceUnixTimestamp+int64(endTimeWindowDelta), 0).UTC()
}

//WindowURL returns windowURL
func (b *Batch) WindowURL(baseURL, dest string, sourceTime time.Time) string {
	endTime := b.WindowEndTime(sourceTime)
	return url.Join(baseURL, fmt.Sprintf("%v_%v%v", dest, endTime.Unix(), shared.WindowExt))
}

//NewBatch creates a batch
func NewBatch(durationInSec int) *Batch {
	return &Batch{
		Window: &Window{
			DurationInSec: durationInSec,
			Duration:      time.Second * time.Duration(durationInSec),
		},
	}
}
