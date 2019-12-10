package replay

import (
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
	"strings"
	"time"
)

const (
	defaultTriggerAge = "1hour"
	agoKeyword        = "Ago"
)

//Request represents reply request
type Request struct {
	TriggerURL                string
	ReplayBucket              string
	UnprocessedDuration       string
	unprocessedModifiedBefore *time.Time
}

//Response represents replay response
type Response struct {
	Replayed []string
	Status   string
	Error    string
}

//Init initialises request
func (r *Request) Init() (err error) {
	if r.UnprocessedDuration == "" {
		r.UnprocessedDuration = defaultTriggerAge
	}
	if !(strings.Contains(strings.ToLower(r.UnprocessedDuration), "ago") || strings.Contains(strings.ToLower(r.UnprocessedDuration), "past")) {
		r.UnprocessedDuration += agoKeyword
	}
	if r.unprocessedModifiedBefore, err = toolbox.TimeAt(r.UnprocessedDuration); err != nil {
		return errors.Wrapf(err, "invalid UnprocessedDuration: %v", r.UnprocessedDuration)
	}
	return nil
}

//Validate check if request is valid
func (r *Request) Validate() error {
	if r.ReplayBucket == "" {
		return errors.New("replayBucket was empty")
	}
	if r.TriggerURL == "" {
		return errors.New("triggerURL was empty")
	}

	return nil
}
