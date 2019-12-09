package mon

import (
	"bqtail/base"
	"bqtail/mon/info"
	"github.com/viant/toolbox"
	"strings"
	"time"
)

type batches []*batch

func (b batches) groupByDest() map[string]*batch {
	var result = make(map[string]*batch, 0)
	for i, batch := range b {
		_, ok := result[batch.dest]
		if !ok {
			result[batch.dest] = b[i]
		}
		batch.AddEvent(batch.dueToRun)
	}
	return result
}

type batch struct {
	dest string
	info.Metric
	dueToRun time.Time
}

func parseBatch(encoded string) *batch {
	encoded = strings.Replace(encoded, base.WindowExt, "", 1)
	elements := strings.Split(encoded, "_")
	tableLimit := len(elements) - 1
	dueRunUnixTs := elements[tableLimit]
	_, errInt := toolbox.ToInt(elements[tableLimit-1])
	if errInt == nil && len(elements[tableLimit-1]) > 15 {
		tableLimit--
	}
	dest := strings.Join(elements[:tableLimit], "_")
	if ref, err := base.NewTableReference(dest); err == nil {
		dest = ref.DatasetId + "." + ref.TableId
	}
	result := &batch{
		dueToRun: time.Unix(int64(toolbox.AsInt(dueRunUnixTs)), 0),
		dest:     dest,
	}
	return result
}
