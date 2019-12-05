package mon

import (
	"bqtail/base"
	"github.com/viant/toolbox"
	"strings"
	"time"
)


type batches []*batch




type batch struct {
	dest   string
	dueRun time.Time
}


func parseBatch(encoded string) *batch {
	encoded = strings.Replace(encoded, base.WindowExt, "", 1)
	elements := strings.Split(encoded, "_")
	tableLimit := len(elements)-1
	dueRunUnixTs := elements[tableLimit]
	_, errInt := toolbox.ToInt(elements[tableLimit-1])
	if errInt == nil && len(elements[tableLimit-1]) > 15 {
		tableLimit--
	}
	result := &batch{
		dueRun: time.Unix(int64(toolbox.AsInt(dueRunUnixTs)), 0),
		dest:   strings.Join(elements[:tableLimit], "_"),
	}
	return result
}

