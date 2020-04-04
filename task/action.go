package task

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/stage/activity"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"google.golang.org/api/bigquery/v2"
	"io/ioutil"
	"strings"
)

//Action represents route action
type Action struct {
	Action         string                 `json:",omitempty"`
	Meta           *activity.Meta         `json:",omitempty"`
	Request        map[string]interface{} `json:",omitempty"`
	serviceRequest interface{}
	*Actions       `json:",omitempty"`
}

//Init initialises action
func (a *Action) Init(ctx context.Context, fs afs.Service) error {
	if a.Request == nil {
		a.Request = make(map[string]interface{})
	}
	isEmptyRequest := len(a.Request) == 0
	if isEmptyRequest {
		switch a.Action {
		case shared.ActionDelete:
			_, hasURL := a.Request[shared.URLsKey]
			_, hasURLs := a.Request[shared.URLKey]
			if !(hasURL || hasURLs) {
				a.Request[shared.URLsKey] = shared.LoadURIsVar
			}
		}
	} else {

		if request, err := toolbox.NormalizeKVPairs(a.Request); err == nil {
			a.Request = toolbox.AsMap(request)
		}
	}
	if a.Action == shared.ActionMove {
		if _, ok := a.Request[shared.SourceURLsKey]; !ok {
			if _, ok := a.Request[shared.SourceURLKey]; !ok {
				a.Request[shared.SourceURLsKey] = shared.LoadURIsVar
			}
		}
	}

	if a.Action == shared.ActionCall {
		if err := loadResource(ctx, a, fs, "Body", "BodyURL"); err != nil {
			return err
		}
	}
	if a.Action == shared.ActionQuery {
		if err := loadResource(ctx, a, fs, "SQL", "SQLURL"); err != nil {
			return err
		}
	}
	if shared.Actionable[a.Action] {
		if a.Actions == nil {
			a.Actions = &Actions{}
		}
	} else {
		if a.Actions != nil && (len(a.Actions.OnSuccess) > 0 || len(a.Actions.OnFailure) > 0) {
			return errors.Errorf("action %v does not support OnSuccess/OnFailure", a.Action)
		}
		a.Actions = nil
		return nil
	}
	return a.Actions.Init(ctx, fs)
}

//ServiceRequest returns a service request
func (a Action) ServiceRequest() interface{} {
	return a.serviceRequest
}

//RequestValue returns request value for supplied key
func (a Action) RequestValue(key string) interface{} {
	if len(a.Request) == 0 {
		return nil
	}
	value, ok := a.Request[key]
	if ok {
		return value
	}
	key = strings.ToLower(key)
	for k, v := range a.Request {
		if strings.ToLower(k) == key {
			return v
		}
	}
	return ""
}

//RequestStringValue returns request string value for supplied key
func (a Action) RequestStringValue(key string) string {
	value := a.RequestValue(key)
	if value == nil {
		return ""
	}
	return toolbox.AsString(value)
}

//SetRequest set request for supplied req instance
func (a *Action) SetRequest(req interface{}) error {
	var err error
	if aMap, ok := req.(map[string]interface{}); ok {
		a.Request = aMap
	} else {
		a.Request = map[string]interface{}{}
		err = toolbox.DefaultConverter.AssignConverted(&a.Request, req)
		a.serviceRequest = req
	}
	return err
}

//Expand creates a new expanded action with expander
func (a Action) Expand(root *stage.Process, expander data.Map) *Action {
	var step *activity.Meta
	if root != nil {
		step = activity.New(root, a.Action, root.Mode(a.Action), root.IncStepCount())
	}
	var result = &Action{
		Action: a.Action,
	}
	expanded := expander.Expand(a.Request)
	result.Request = toolbox.AsMap(expanded)
	if !shared.Actionable[a.Action] {
		return result
	}
	result.Meta = step
	result.Actions = &Actions{
		OnSuccess: make([]*Action, 0),
		OnFailure: make([]*Action, 0),
	}
	if a.Actions != nil {
		a.Actions.expandActions(root, expander, result.Actions)
	}
	return result
}

//JobReference returns a job reference
func (a Action) JobReference() *bigquery.JobReference {
	return &bigquery.JobReference{
		Location:  a.Meta.Region,
		JobId:     a.Meta.GetJobID(),
		ProjectId: a.Meta.ProjectID,
	}
}

//NewAction creates a new action for supplied name, action
func NewAction(action string, req interface{}) (*Action, error) {
	result := &Action{Action: action}
	err := result.SetRequest(req)
	return result, err
}

//NewActionFromURL create a new actions from URL
func NewActionFromURL(ctx context.Context, fs afs.Service, URL string) (action *Action, err error) {
	err = base.RunWithRetries(func() error {
		action, err = newActionFromURL(fs, ctx, URL)
		return err
	})
	return action, err
}

func newActionFromURL(fs afs.Service, ctx context.Context, URL string) (*Action, error) {
	result := &Action{}
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download: %v", URL)
	}
	defer func() {
		_ = reader.Close()
	}()
	actionData, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "fail to read post actions: %v", URL)
	}
	err = json.Unmarshal(actionData, &result)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal: %s", actionData)
	}
	return result, err
}
