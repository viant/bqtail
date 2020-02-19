package dispatch

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
	"time"
)

//URLToWindowEndTime converts SourceURL to batch window end time
func URLToWindowEndTime(URL string) (*time.Time, error) {
	index := strings.LastIndex(URL, "_")
	if index == -1 {
		return nil, fmt.Errorf("invalid SourceURL: %v", URL)
	}
	unixFragment := string(URL[index+1 : len(URL)-4])
	unixTimestamp, err := toolbox.ToInt(unixFragment)
	if err != nil {
		return nil, err
	}
	ts := time.Unix(int64(unixTimestamp), 0)
	return &ts, nil
}
