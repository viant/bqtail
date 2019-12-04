package tail

import (
	"bqtail/base"
	"bqtail/stage"
	"bqtail/task"
	"github.com/viant/toolbox"
	"math/rand"
	"time"
)

const (
	recoverJobPrefix = "recover"
)

func getRandom(min, max int) int {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return min + int(rnd.Int63())%(max-min)
}

func updateJobID(eventID, jobID string) string {
	info := stage.Parse(jobID)
	info.EventID = eventID
	return info.GetJobID()
}

func buildJobIDReplacementMap(eventID string, actions []*task.Action) map[string]interface{} {
	var result = make(map[string]interface{})
	for i, action := range actions {
		jobID, ok := action.Request[base.JobIDKey]
		if ok {
			info := stage.Parse(toolbox.AsString(jobID))
			info.EventID = eventID
			info.Step = i + 1
			result[base.JobIDKey] = info.GetJobID()
			break
		}
	}
	return result
}
