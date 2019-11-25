package batch

import (
	"bqtail/tail/config"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"io/ioutil"
	"strconv"
	"time"
)

type BatchedWindow struct {
	*Window
	OwnerEventID string
}

//Window represent batching window
type Window struct {
	EventID string `json:",omitempty"`
	URL     string `json:",omitempty"`
	*config.Rule
	SourceURL string    `json:",omitempty"`
	Start     time.Time `json:",omitempty"`
	End       time.Time `json:",omitempty"`
	URIs      []string  `json:",omitempty"`
}

//NewWindow create a stage batch window
func NewWindow(eventID string, startTime, endTime time.Time, sourceURL, windowURL string, rule *config.Rule) *Window {
	return &Window{
		URL:       windowURL,
		SourceURL: sourceURL,
		Rule:      rule,
		EventID:   eventID,
		Start:     startTime,
		End:       endTime,
	}
}

func windowToTime(window storage.Object) (*time.Time, error) {
	result, err := windowNameToTime(window.Name())
	if err != nil {
		return nil, errors.Wrapf(err, "invalid nano time for URL: %v", window.URL())
	}
	return result, nil
}

func windowNameToTime(name string) (*time.Time, error) {
	unixTimestampLiteral := string(name[:len(name)-4])
	unixTimestamp, err := strconv.ParseInt(unixTimestampLiteral, 10, 64)
	if err != nil {
		return nil, err
	}
	result := time.Unix(unixTimestamp, 0)
	return &result, nil
}

func GetWindow(ctx context.Context, URL string, fs afs.Service) (*Window, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
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
