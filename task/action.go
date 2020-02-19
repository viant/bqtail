package task

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
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

func (a Action) ServiceRequest() interface{} {
	return a.serviceRequest
}

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

//New creates a new action
func (a Action) Expand(root *stage.Process, expander data.Map) *Action {
	var step *activity.Meta
	if root != nil {
		step = activity.New(root, a.Action, root.ActionSuffix(a.Action), root.IncStepCount())
	}
	var result = &Action{
		Action: a.Action,
	}
	expanded := expander.Expand(a.Request)
	result.Request = toolbox.AsMap(expanded)
	if ! shared.Actionable[a.Action] {
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

//NewActionFromURL create a new actions from SourceURL
func NewActionFromURL(ctx context.Context, fs afs.Service, URL string) (*Action, error) {
	result := &Action{}
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		err = errors.Wrapf(err, "failed to unmarshal: %s", data)
	}
	return result, err
}
