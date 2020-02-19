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

//Init initialises actions
func (a *Actions) Init(ctx context.Context, fs afs.Service) error {
	err := a.init(ctx, fs, a.OnSuccess)
	if err != nil {
		return err
	}
	return a.init(ctx, fs, a.OnFailure)
}

func (a *Actions) init(ctx context.Context, fs afs.Service, actions []*Action) (err error) {
	if len(actions) == 0 {
		return nil
	}
	for i, action := range actions {
		if action.Action == "" {
			return errors.Errorf("action was empty: %+v", action)
		}
		if shared.Actionable[action.Action] {
			if action.Actions == nil {
				action.Actions = &Actions{}
			}
		} else {
			if action.Actions != nil && (len(action.Actions.OnSuccess) > 0 || len(action.Actions.OnFailure) > 0) {
				return errors.Errorf("action %v does not support OnSuccess/OnFailure")
			}
			action.Actions = nil
		}

		if action.Request == nil {
			action.Request = make(map[string]interface{})
		}

		ok := len(action.Request) == 0
		if ok {
			switch action.Action {
			case shared.ActionDelete, shared.ActionMove:
				_, hasURL := action.Request[shared.URLsKey]
				_, hasURLs := action.Request[shared.URLKey]
				if ! (hasURL || hasURLs) {
					action.Request[shared.URLsKey] = shared.LoadURIsVar
				}
			}
		}

		if action.Actions != nil {
			if len(action.OnSuccess) > 0 {
				for k := range action.OnSuccess {
					onSuccess := actions[i].OnSuccess[k]
					if onSuccess.Actions != nil {
						if err = onSuccess.init(ctx, fs, onSuccess.OnSuccess); err != nil {
							return err
						}
					}
				}
			}
			if len(action.OnFailure) > 0 {
				for k := range action.OnFailure {
					onFailure := actions[i].OnFailure[k]
					if onFailure.Actions != nil {
						if err = onFailure.init(ctx, fs, onFailure.OnFailure); err != nil {
							return err
						}
					}
				}
			}
		}

		if action.Action == shared.ActionCall {
			if err := loadResource(actions[i], ctx, fs, "Body", "BodyURL"); err != nil {
				return err
			}
			continue
		}
		if action.Action == shared.ActionQuery {
			if err := loadResource(actions[i], ctx, fs, "SQL", "SQLURL"); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadResource(action *Action, ctx context.Context, fs afs.Service, dataKey, URLKey string) error {
	body := action.RequestStringValue(dataKey)
	if body != "" {
		return nil
	}
	bodyURL := action.RequestStringValue(URLKey)
	if bodyURL == "" {
		return nil
	}
	body, err := load(ctx, fs, bodyURL)
	if err != nil {
		return err
	}
	action.Request[dataKey] = body
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
		//childInfo := a.Meta.ChildInfo(toRun[i].Action, i+1)
		//toRun[i].Request[shared.JobIDKey] = childInfo.GetJobID()
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
