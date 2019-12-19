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

//SetInfo sets action info
func (a *Actions) SetInfo(info *stage.Info) {
	a.Info = *info
}

//CloneOnFailure returns actions to run
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
func (a Actions) ToRun(err error, job *base.Job) []*Action {
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
		if _, ok := toRun[i].Request[base.JobSourceKey]; !ok {
			toRun[i].Request[base.JobSourceKey] = job.Source()
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
func (a *Actions) Expand(info *stage.Info) *Actions {
	a.SetInfo(info)
	result := &Actions{
		Info:      a.Info,
		OnSuccess: make([]*Action, 0),
		OnFailure: make([]*Action, 0),
	}
	appendSourceURLinfoActions(a.OnSuccess, &result.OnSuccess, info)
	appendSourceURLinfoActions(a.OnFailure, &result.OnFailure, info)
	appendSourceURLNoninfoActions(a.OnSuccess, &result.OnSuccess, info)
	appendSourceURLNoninfoActions(a.OnFailure, &result.OnFailure, info)
	expandSource(result.OnSuccess, info)
	expandSource(result.OnFailure, info)
	return result
}

func expandSource(actions []*Action, info *stage.Info) {
	if info.TempTable == "" {
		return
	}
	for i := range actions {
		if _, has := actions[i].Request[base.JobSourceKey]; !has {
			actions[i].Request[base.JobSourceKey] = info.TempTable
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

func appendSourceURLinfoActions(source []*Action, dest *[]*Action, info *stage.Info) {
	if len(source) == 0 {
		return
	}
	if len(source) == 1 && source[0].Action == base.ActionDelete {
		*dest = append(*dest, source[0].New(info, map[string]interface{}{
			base.URLsKey: info.LoadURIs,
		}))
		return
	}

	if len(info.LoadURIs) > 0 {
		for i := range info.LoadURIs {
			for _, action := range source {
				if !sourceURLExpandable[action.Action] {
					continue
				}
				*dest = append(*dest, action.New(info, map[string]interface{}{
					base.URLsKey: []string{info.LoadURIs[i]},
				}))
			}
		}
		return
	}
}

func appendSourceURLNoninfoActions(source []*Action, dest *[]*Action, info *stage.Info) {
	for _, action := range source {
		if sourceURLExpandable[action.Action] {
			continue
		}
		*dest = append(*dest, action.New(info, map[string]interface{}{}))
	}
}
