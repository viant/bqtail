package task

import (
	"bqtail/base"
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
)

//Actions represents actions
type Actions struct {
	Job          *bigquery.Job
	SourceURL    string
	DeferTaskURL string    `json:",omitempty"`
	Async        bool      `json:",omitempty"`
	JobID        string    `json:",omitempty"`
	OnSuccess    []*Action `json:",omitempty"`
	OnFailure    []*Action `json:",omitempty"`
}

//ToRun returns actions to run
func (a Actions) CloneOnFailure() *Actions {
	result := &Actions{
		SourceURL:    a.SourceURL,
		DeferTaskURL: a.DeferTaskURL,
		Async:        a.Async,
		JobID:        a.JobID,
		OnFailure:    a.OnFailure,
	}
	return result
}

var actionMapping = map[string]string{
	"copy":  "cp",
	"query": "sql",
}

//ToRun returns actions to run
func (a Actions) ToRun(err error, job *base.Job, deferredURL string) []*Action {
	var toRun []*Action

	if err == nil {
		toRun = append([]*Action{}, a.OnSuccess...)

	} else {
		toRun = append([]*Action{}, a.OnFailure...)
		e := err.Error()
		for i := range toRun {
			toRun[i].Request[base.ErrorKey] = e
		}
	}

	for i := range toRun {

		jobSuffix := ""
		if !a.Async {
			jobSuffix = "_sync"
		}
		if hasPostTasks := toRun[i].Request[base.OnSuccessKey] != nil || toRun[i].Request[base.OnFailureKey] != nil; !hasPostTasks {
			jobSuffix = "_nop"
		}

		if bqJobs[toRun[i].Action] {
			actionName := toRun[i].Action
			if val, ok := actionMapping[actionName]; ok {
				actionName = val
			}
			toRun[i].Request[base.JobIDKey] = job.ChildJobID(fmt.Sprintf("%02d_%v", i, actionName) + jobSuffix)
		}

		if bodyAppendable[toRun[i].Action] {
			if responseJSON, err := json.Marshal(a); err == nil {
				toRun[i].Request[base.ResponseKey] = string(responseJSON)
			}
		}
		if _, ok := toRun[i].Request[base.JobIDKey]; !ok {
			toRun[i].Request[base.JobIDKey] = job.JobID()
		}
		if _, ok := toRun[i].Request[base.EventIDKey]; !ok {
			toRun[i].Request[base.EventIDKey] = job.EventID()
		}
		if _, ok := toRun[i].Request[base.DestTableKey]; !ok {
			toRun[i].Request[base.DestTableKey] = job.DestTable()
		}
		if _, ok := toRun[i].Request[base.SourceTableKey]; !ok {
			toRun[i].Request[base.SourceTableKey] = job.SourceTable()
		}
		if _, ok := toRun[i].Request[base.DeferTaskURL]; !ok {
			toRun[i].Request[base.DeferTaskURL] = deferredURL
		}
		if _, ok := toRun[i].Request[base.SourceKey]; !ok {
			toRun[i].Request[base.SourceKey] = job.Source()
		}

	}
	return toRun
}

//IsEmpty returns is actions are empty
func (a Actions) IsEmpty() bool {
	return len(a.OnSuccess) == 0 && len(a.OnFailure) == 0
}

//ID returns actions ID
func (a Actions) ID(prefix string) (string, error) {
	if a.JobID != "" {
		return a.JobID, nil
	}
	var err error
	a.JobID, err = NextID(prefix)
	return a.JobID, err
}

//IsSyncMode returns true if route uses synchronous mode
func (a Actions) IsSyncMode() bool {
	return !a.Async
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
	if len(expandable.SourceURLs) > 0 {
		result.SourceURL = expandable.SourceURLs[0]
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
func (a *Actions) AddOnSuccess(actions ...*Action) {
	if len(a.OnSuccess) == 0 {
		a.OnSuccess = make([]*Action, 0)
	}
	for i := range actions {
		a.OnSuccess = append(a.OnSuccess, actions[i])
	}
}

//AddOnFailure adds on failure action
func (a *Actions) AddOnFailure(actions ...*Action) {
	if len(a.OnFailure) == 0 {
		a.OnFailure = make([]*Action, 0)
	}
	for i := range actions {
		a.OnFailure = append(a.OnFailure, actions[i])
	}
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

func appendSourceURLExpandableActions(source []*Action, dest *[]*Action, expandable *base.Expandable) {
	if len(source) == 0 {
		return
	}

	if len(source) == 1 && source[0].Action == "delete" {
		*dest = append(*dest, source[0].New(map[string]interface{}{
			base.URLsKey: expandable.SourceURLs,
		}))
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
