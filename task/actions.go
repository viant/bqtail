package task

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/toolbox/data"
	"google.golang.org/api/bigquery/v2"
)

//Actions represents actions
type Actions struct {
	Job       *bigquery.Job `json:",omitempty"`
	OnSuccess []*Action     `json:",omitempty"`
	OnFailure []*Action     `json:",omitempty"`
}

func (a Actions) Clone() *Actions {
	result := &Actions{
		Job: a.Job,
	}
	if len(a.OnFailure) > 0 {
		result.OnFailure = make([]*Action, len(a.OnFailure))
		for i := range a.OnFailure {
			result.OnFailure[i] = a.OnFailure[i].Clone()
		}
	}
	if len(a.OnSuccess) > 0 {
		result.OnSuccess = make([]*Action, len(a.OnSuccess))
		for i := range a.OnSuccess {
			result.OnSuccess[i] = a.OnSuccess[i].Clone()
		}
	}
	return result
}

//Init initialises actions
func (a *Actions) Init(ctx context.Context, fs afs.Service) error {
	if a == nil {
		return nil
	}
	if err := initActions(ctx, fs, a.OnSuccess); err != nil {
		return err
	}
	return initActions(ctx, fs, a.OnFailure)
}

func initActions(ctx context.Context, fs afs.Service, actions []*Action) (err error) {
	if len(actions) == 0 {
		return nil
	}
	for _, action := range actions {
		if action.Action == "" {
			return errors.Errorf("action was empty: %+v", action)
		}

		if err := action.Init(ctx, fs); err != nil {
			return err
		}

	}
	return nil
}

func loadResource(ctx context.Context, action *Action, fs afs.Service, dataKey, URLKey string) error {
	body := action.RequestStringValue(dataKey)
	if body != "" {
		return nil
	}
	bodyURL := action.RequestStringValue(URLKey)
	if bodyURL == "" {
		return nil
	}
	data, err := fs.DownloadWithURL(ctx, bodyURL)
	if err != nil {
		return err
	}
	action.Request[dataKey] = string(data)
	return nil
}

//CloneOnFailure returns actions to run
func (a Actions) CloneOnFailure() *Actions {
	result := &Actions{
		OnFailure: a.OnFailure,
	}
	return result
}

//ToRun returns actions to run
func (a Actions) ToRun(err error, job *base.Job) []*Action {
	var toRun []*Action
	if err == nil {
		toRun = append([]*Action{}, a.OnSuccess...)

	} else {
		toRun = append([]*Action{}, a.OnFailure...)
		errorMessage := err.Error()
		for i := range toRun {
			toRun[i].Request[shared.ErrorKey] = errorMessage
		}
	}

	for i := range toRun {
		if toRun[i].Action == shared.ActionNotify || toRun[i].Action == shared.ActionCall {
			if responseJSON, err := json.Marshal(a); err == nil {
				toRun[i].Request[shared.ResponseKey] = string(responseJSON)
			}
		}
		if _, ok := toRun[i].Request[shared.JobSourceKey]; !ok {
			toRun[i].Request[shared.JobSourceKey] = job.Source()
		}
	}
	return toRun
}

//IsEmpty returns is actions are empty
func (a Actions) IsEmpty() bool {
	return len(a.OnSuccess) == 0 && len(a.OnFailure) == 0
}

//Expand creates clone actions with expanded sources URLs
func (a *Actions) Expand(root *stage.Process, action string, sourceURIs []string) *Actions {
	expander := root.Expander(sourceURIs)
	result := &Actions{
		OnSuccess: make([]*Action, 0),
		OnFailure: make([]*Action, 0),
	}
	a.expandActions(root, expander, result)
	return result
}

func (a *Actions) expandActions(root *stage.Process, expander data.Map, result *Actions) {
	if a == nil {
		return
	}

	if len(a.OnSuccess) > 0 {
		result.OnSuccess = expandActions(root, a.OnSuccess, expander)
	}
	if len(a.OnFailure) > 0 {
		result.OnFailure = expandActions(root, a.OnFailure, expander)
	}
}

func expandActions(root *stage.Process, actions []*Action, expander data.Map) []*Action {
	var result = make([]*Action, 0)
	for _, action := range actions {
		action := action.Expand(root, expander)
		result = append(result, action)
	}
	return result
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

func (a *Actions) FinalizeOnSuccess(actions ...*Action) {
	if len(a.OnSuccess) == 0 {
		a.OnSuccess = make([]*Action, 0)
	}
	finalized := false
	postActionCount := len(a.OnSuccess)
outer:
	for i := 0; i < postActionCount; i++ {
		action := a.OnSuccess[postActionCount-(i+1)]
		switch postActionCount {
		case 1:
			if action.Action == shared.ActionQuery || action.Action == shared.ActionExport || action.Action == shared.ActionCopy {
				finalized = true
				action.FinalizeOnSuccess(actions...)
				break outer
			}
		default:
			if action.Action == shared.ActionQuery {
				finalized = true
				action.FinalizeOnSuccess(actions...)
				break outer
			}
		}
	}
	if !finalized {
		a.OnSuccess = append(a.OnSuccess, actions...)
	}
}

//NewActions creates an actions
func NewActions(onSuccess, onFailure []*Action) *Actions {
	if len(onSuccess) == 0 {
		onSuccess = make([]*Action, 0)
	}
	if len(onFailure) == 0 {
		onFailure = make([]*Action, 0)
	}
	return &Actions{
		OnSuccess: onSuccess,
		OnFailure: onFailure,
	}
}
