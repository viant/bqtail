package task

import (
	"bqtail/base"
)

type Actions struct {
	DeferTaskURL string    `json:",ommittempty"`
	Async        bool      `json:",ommittempty"`
	JobID        string    `json:",ommittempty"`
	OnSuccess    []*Action `json:",ommittempty"`
	OnFailure    []*Action `json:",ommittempty"`
}

//IsEmpty returns is actions are empty
func (a Actions) IsEmpty() bool {
	return len(a.OnSuccess) == 0 && len(a.OnFailure) == 0
}

//ID returns actions ID
func (r Actions) ID(prefix string) (string, error) {
	if r.JobID != "" {
		return r.JobID, nil
	}
	var err error
	r.JobID, err = NextID(prefix)
	return r.JobID, err
}

//IsSyncMode returns true if route uses synchronous mode
func (r Actions) IsSyncMode() bool {
	return !r.Async
}

//Expand creates clone actions with expanded sources URLs
func (a *Actions) Expand(expandable *base.Expandable) *Actions {
	if expandable == nil {
		return a
	}
	result := &Actions{
		Async:        a.Async,
		DeferTaskURL: a.DeferTaskURL,
		JobID:        a.JobID,
		OnSuccess:    make([]*Action, 0),
		OnFailure:    make([]*Action, 0),
	}
	appendSourceURLExpandableActions(a.OnSuccess, &result.OnSuccess, expandable)
	appendSourceURLExpandableActions(a.OnFailure, &result.OnFailure, expandable)
	appendSourceURLNonExpandableActions(a.OnSuccess, &result.OnSuccess)
	appendSourceURLNonExpandableActions(a.OnFailure, &result.OnFailure)
	expandSource(result.OnSuccess, expandable)
	expandSource(result.OnFailure, expandable)
	return result
}

func expandSource(actions []*Action, expandable *base.Expandable) {
	if expandable.Source == "" {
		return
	}
	for i := range actions {
		if _, has := actions[i].Request[base.SourceKey]; !has {
			actions[i].Request[base.SourceKey] = expandable.Source
		}
	}
}

//AddOnSuccess adds on sucess action
func (a *Actions) AddOnSuccess(action *Action) {
	if len(a.OnSuccess) == 0 {
		a.OnSuccess = make([]*Action, 0)
	}
	a.OnSuccess = append(a.OnSuccess, action)
}

//AddOnFailure adds on failure action
func (a *Actions) AddOnFailure(action *Action) {
	if len(a.OnFailure) == 0 {
		a.OnFailure = make([]*Action, 0)
	}
	a.OnFailure = append(a.OnFailure, action)
}

//NewActions creates an actions
func NewActions(async bool, baseURL, jobID string, onSuccess, onFailure []*Action) *Actions {
	if len(onSuccess) == 0 {
		onSuccess = make([]*Action, 0)
	}
	if len(onFailure) == 0 {
		onFailure = make([]*Action, 0)
	}
	return &Actions{
		Async:        async,
		DeferTaskURL: baseURL,
		JobID:        jobID,
		OnSuccess:    onSuccess,
		OnFailure:    onFailure,
	}
}

var sourceURLExpandable = map[string]bool{
	"move":   true,
	"delete": true,
}

func appendSourceURLExpandableActions(source []*Action, dest *[]*Action, expandable *base.Expandable) {
	if len(source) == 0 {
		return
	}
	if len(expandable.SourceURLs) > 0 {
		for i := range expandable.SourceURLs {
			for _, action := range source {
				if !sourceURLExpandable[action.Action] {
					continue
				}
				*dest = append(*dest, action.New(map[string]interface{}{
					base.SourceURLKey: expandable.SourceURLs[i],
				}))

			}
		}
		return
	}
}

func appendSourceURLNonExpandableActions(source []*Action, dest *[]*Action) {
	for _, action := range source {
		if sourceURLExpandable[action.Action] {
			continue
		}
		*dest = append(*dest, action.New(map[string]interface{}{}))
	}
}
