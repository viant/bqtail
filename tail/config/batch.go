package config

import (
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/shared"
	"time"
)

//Batch transfer config
type Batch struct {
	//Window batch time window
	Window *Window `json:",omitempty"`
	//RollOver if this flag is set, if the first event of the batch fall outside of the first half time, the window can be expanded if previous window had not existed.
	RollOver bool `json:",omitempty"`
	//Batch base URL
	BaseURL string `json:",omitempty"`

	//MultiPath is one batch can collect files from various folder
	MultiPath bool `json:",omitempty"`

	//MaxDelayInSec delay before collecting batch file. to randomly distribute workload,
	// when a table has 40 shards, 40 batches would start exactly at the same time unless this parameter is specified
	MaxDelayInSec int `json:",omitempty"`
}

//Init initialises batch mode
func (b *Batch) Init(baseURL string) {
	if b.Window == nil {
		b.Window = &Window{}
	}
	b.Window.Init()
	if b.BaseURL == "" {
		b.BaseURL = baseURL
	}
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
	return time.Unix(sourceUnixTimestamp+int64(endTimeWindowDelta), 0)
}

//WindowURL returns windowURL
func (b *Batch) WindowURL(dest string, sourceTime time.Time) string {
	endTime := b.WindowEndTime(sourceTime)
	return url.Join(b.BaseURL, fmt.Sprintf("%v_%v%v", dest, endTime.Unix(), shared.WindowExt))
}

//NewBatch creates a batch
func NewBatch(durationInSec int, baseURL string) *Batch {
	return &Batch{
		BaseURL: baseURL,
		Window: &Window{
			DurationInSec: durationInSec,
			Duration:      time.Second * time.Duration(durationInSec),
		},
	}
}
