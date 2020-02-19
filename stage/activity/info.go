package activity

import (
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"fmt"
	"github.com/viant/toolbox"
	"path"
	"strings"
	"time"
)

const (

	//URLsKey URLs keys
	URLsKey = "URLs"
)

//Meta represents processing stage meta data
type Meta struct {
	stage.Process
	Counter int    `json:",omitempty"`
	Action  string `json:",omitempty"`
	Mode    string `json:",omitempty"`
	Step    int    `json:",omitempty"`
}



//ID returns stage ID
func (i *Meta) ID() string {
	return path.Join(i.DestTable, fmt.Sprintf("%v_%05d_%v", i.EventID, i.Step%99999, i.Action)+shared.PathElementSeparator+i.Mode)
}

//JobFilename returns job filename
func (i *Meta) JobFilename() string {
	dest := i.DestTable
	if dest != "" {
		dest += shared.PathElementSeparator
	}
	baseLocation := ""
	if i.ProjectID != "" {
		baseLocation = shared.TempProjectPrefix + i.ProjectID + ":" + i.Region + "/"
	}
	return baseLocation + dest + fmt.Sprintf("%v_%05d_%v", i.EventID, i.Step%99999, i.Action) + shared.PathElementSeparator + i.Mode
}


//Sequence returns step sequence
func (i *Meta) Sequence() int {
	upper := (i.Step / 1000)
	lower := (i.Step % 100)
	return upper + lower
}

//Wrap wraps info
func (i *Meta) Wrap(action string) *Meta {
	suffix := i.Mode
	return &Meta{
		Process: i.Process,
		Mode:    suffix,
		Step:    i.Step + 1,
		Action:  action,
	}
}

//GetJobID returns  a job ID
func (i *Meta) GetJobID() string {
	ID := i.JobFilename()
	if strings.Contains(ID, shared.TempProjectPrefix) {
		if index := strings.Index(ID, "/"); index != -1 {
			ID = string(ID[index+1:])
		}
	}
	return Decode(ID)
}

//Decode decode path base ID to big query Job ID
func Decode(jobID string) string {
	if count := strings.Count(jobID, "/"); count > 0 {
		jobID = strings.Replace(jobID, "/", shared.PathElementSeparator, count)
	}
	if count := strings.Count(jobID, "$"); count > 0 {
		jobID = strings.Replace(jobID, "$", "_", count)
	}
	if count := strings.Count(jobID, ":"); count > 0 {
		jobID = strings.Replace(jobID, ":", "_", count)
	}
	if count := strings.Count(jobID, "."); count > 0 {
		jobID = strings.Replace(jobID, ".", "_", count)
	}
	return jobID
}

//Parse parse encoded job ID
func Parse(encoded string) *Meta {
	encoded = strings.Replace(encoded, ".json", "", 1)
	encoded = strings.Replace(encoded, shared.PathElementSeparator, "/", strings.Count(encoded, shared.PathElementSeparator))
	result := &Meta{
		Mode:   shared.StepModeTail,
		Action: shared.StepModeNop,
	}

	if strings.HasSuffix(encoded, shared.StepModeDispach) {
		result.Mode = shared.StepModeDispach
	}
	elements := strings.Split(encoded, "/")
	if len(elements) < 2 {
		result.DestTable = strings.Join(elements[:len(elements)-1], shared.PathElementSeparator)
		result.EventID = fmt.Sprintf("%v", time.Now().Nanosecond()%10000)
	} else {
		eventOffset := len(elements) - 2
		eventElements := strings.Split(elements[eventOffset], "_")
		result.DestTable = strings.Join(elements[:eventOffset], shared.PathElementSeparator)

		result.EventID = eventElements[0]
		if len(eventElements) > 2 {
			result.EventID = eventElements[0]
			result.Step = toolbox.AsInt(eventElements[1])
			result.Action = eventElements[2]
		}
	}
	result.Async = strings.HasSuffix(result.Mode, shared.StepModeDispach)
	return result
}



//New create a processing meta info
func New(process *stage.Process, action, suffix string, step int) *Meta {
	return &Meta{
		Process: *process,
		Action:  action,
		Mode:    suffix,
		Step:    step,
	}
}
