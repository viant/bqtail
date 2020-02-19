package batch

import (
	"github.com/viant/bqtail/stage"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"io/ioutil"
	"time"
)

//Process represents batch info, process can be owner of be owner by existing batch
type Info struct {
	*Window
	OwnerEventID string
}

//Window represent batching window
type Window struct {
	*stage.Process
	URL       string `json:",omitempty"`
	Start     time.Time `json:",omitempty"`
	End       time.Time `json:",omitempty"`
	URIs      []string  `json:",omitempty"`
	Locations []string  `json:",omitempty"`
}

//NewWindow create a stage batch window
func NewWindow(process *stage.Process, startTime, endTime time.Time,  windowURL string) *Window {
	return &Window{
		Process: process,
		URL:     windowURL,
		Start:   startTime,
		End:     endTime,
	}
}

//GetWindow returns a batch window or erro
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
