package task

import (
	"bqtail/base"
	"bqtail/stage"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"google.golang.org/api/bigquery/v2"
)

//Actions represents actions
type Actions struct {
	Job *bigquery.Job
	stage.Info
	JobID     string
	OnSuccess []*Action `json:",omitempty"`
	OnFailure []*Action `json:",omitempty"`
}

func (a *Actions) SetInfo(info *stage.Info) {
	a.Info = *info
}

//ToRun returns actions to run
func (a Actions) CloneOnFailure() *Actions {
	result := &Actions{
		Info:      a.Info,
		OnFailure: a.OnFailure,
	}
	return result
}

//NewActionFromURL create a new actions from URL
func NewActionFromURL(ctx context.Context, fs afs.Service, URL string) (*Actions, error) {
	actions := &Actions{}
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	if err = json.NewDecoder(reader).Decode(&actions); err != nil {
		return nil, errors.Wrapf(err, "unable decode: %v", URL)
	}
	if actions.JobID != "" && actions.Info.DestTable == "" {
		actions.Info = *stage.Parse(actions.JobID)
	}

	return actions, nil
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
		childInfo := a.Info.ChildInfo(toRun[i].Action, i+1)

		toRun[i].Request[base.JobIDKey] = childInfo.GetJobID()
		for k, v := range childInfo.AsMap() {
			toRun[i].Request[k] = v
		}

		if bodyAppendable[toRun[i].Action] {
			if responseJSON, err := json.Marshal(a); err == nil {
				toRun[i].Request[base.ResponseKey] = string(responseJSON)
			}
		}
		if _, ok := toRun[i].Request[base.EventIDKey]; !ok {
			toRun[i].Request[base.EventIDKey] = a.Info.EventID
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
		Info:      a.Info,
		OnSuccess: make([]*Action, 0),
		OnFailure: make([]*Action, 0),
	}
	if len(expandable.SourceURLs) > 0 {
		result.SourceURI = expandable.SourceURLs[0]
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
func NewActions(async bool, info stage.Info, onSuccess, onFailure []*Action) *Actions {
	if len(onSuccess) == 0 {
		onSuccess = make([]*Action, 0)
	}
	if len(onFailure) == 0 {
		onFailure = make([]*Action, 0)
	}
	return &Actions{
		Info:      info,
		OnSuccess: onSuccess,
		OnFailure: onFailure,
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
