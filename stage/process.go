package stage

import (
	"context"
	"github.com/viant/bqtail/shared"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"github.com/viant/afs"
	"strings"
)

//Process represent an injection process
type Process struct {
	Source         *Source                `json:",omitempty"`
	ProcessURL     string                 `json:",omitempty"`
	DoneProcessURL string                 `json:",omitempty"`
	FailedURL      string                 `json:",omitempty"`
	RuleURL        string                 `json:",omitempty"`
	EventID        string                 `json:",omitempty"`
	ProjectID      string                 `json:",omitempty"`
	Region         string                 `json:",omitempty"`
	Params         map[string]interface{} `json:",omitempty"`
	Async          bool                   `json:",omitempty"`
	TempTable      string                 `json:",omitempty"`
	DestTable      string                 `json:",omitempty"`
	StepCount      int                    `json:",omitempty"`
}

func (p *Process) MoveToFailed(ctx context.Context, fs afs.Service) error {
	return fs.Move(ctx, p.ProcessURL, p.FailedURL)
}

//Mode returns action suffix
func (p *Process) Mode(action string) string {
	switch action {
	case shared.ActionQuery, shared.ActionLoad, shared.ActionReload, shared.ActionCopy, shared.ActionExport:
		if p.Async {
			return shared.StepModeDispach
		}
		return shared.StepModeTail
	}
	return shared.StepModeNop

}

//IncStepCount increments and returns step count
func (p *Process) IncStepCount() int {
	p.StepCount++
	return p.StepCount
}

//Expander retuns expander map
func (p Process) Expander(loadURIs []string) data.Map {
	aMap := data.Map(p.AsMap())
	aMap[shared.JobSourceKey] = p.TempTable
	aMap[shared.URLsKey] = strings.Join(loadURIs, ",")
	aMap[shared.LoadURIsKey] = loadURIs
	return aMap
}

//AsMap returns info map
func (p Process) AsMap() map[string]interface{} {
	aMap := map[string]interface{}{}
	_ = toolbox.DefaultConverter.AssignConverted(&aMap, p)
	if len(p.Params) == 0 {
		return aMap
	}
	for k, v := range p.Params {
		aMap[k] = v
	}
	return aMap
}

//GetOrSetProject initialises project ID
func (p *Process) GetOrSetProject(projectID string) string {
	if p.ProjectID != "" {
		projectID = p.ProjectID
	} else {
		p.ProjectID = projectID
	}
	return projectID
}

//IsSyncMode returns sync mode
func (p *Process) IsSyncMode() bool {
	return !p.Async
}

//NewProcess creates a new process
func NewProcess(eventID string, source *Source, ruleURL string, async bool) *Process {
	return &Process{
		EventID: eventID,
		RuleURL: ruleURL,
		Source:  source,
		Async:   async,
	}
}
