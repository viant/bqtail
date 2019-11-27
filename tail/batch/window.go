package batch

import (
	"bqtail/tail/config"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/matcher"
	"io/ioutil"
	"time"
)

type BatchedWindow struct {
	*Window
	OwnerEventID string
}

//Window represent batching window
type Window struct {
	EventID    string         `json:",omitempty"`
	URL        string         `json:",omitempty"`
	Filter     *matcher.Basic `json:",omitempty"`
	RuleURL    string         `json:",omitempty"`
	Table      string
	SourceTime time.Time `json:",omitempty"`
	SourceURL  string    `json:",omitempty"`
	Start      time.Time `json:",omitempty"`
	End        time.Time `json:",omitempty"`
	URIs       []string  `json:",omitempty"`
	Locations  []string
}

//NewWindow create a stage batch window
func NewWindow(eventID string, dest string, startTime, endTime time.Time, sourceURL string, sourceTime time.Time, windowURL string, rule *config.Rule) *Window {
	return &Window{
		URL:        windowURL,
		Table:      dest,
		SourceURL:  sourceURL,
		SourceTime: sourceTime,
		Filter:     &rule.When,
		RuleURL:    rule.Info.URL,
		EventID:    eventID,
		Start:      startTime,
		End:        endTime,
	}
}

func GetWindow(ctx context.Context, URL string, fs afs.Service) (*Window, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read window: %v", URL)
	}
	window := &Window{}
	err = json.Unmarshal(data, window)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal window: %v", URL)
	}
	return window, nil
}
