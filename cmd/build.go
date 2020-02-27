package cmd

import (
	"bytes"
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs/file"
	"github.com/viant/afs/mem"
	foption "github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/cmd/rule/build"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/task"
	"gopkg.in/yaml.v2"
	"os"
	"path"
)

func (s *service) Build(ctx context.Context, request *build.Request) error {
	request.Init(s.config)
	if request.RuleURL == "" {
		request.RuleURL = url.Join(ruleBaseURL, "rule.yaml")
	}

	rule := &config.Rule{
		Async: true,
		Dest: &config.Destination{
			Transient: &config.Transient{},
		},
		OnSuccess: make([]*task.Action, 0),
		OnFailure: make([]*task.Action, 0),
		Info: base.Info{
			URL:          request.RuleURL,
			LeadEngineer: os.Getenv("USER"),
		},
	}

	rule.OnSuccess = append(rule.OnSuccess, &task.Action{Action: shared.ActionDelete})
	if err := s.initSourceMatch(ctx, rule, request); err != nil {
		return err
	}
	s.initDestination(rule, request)
	s.initdBatch(request, rule)

	if !(request.SourceURL != "" || request.Validate) {
		s.reportRule(rule)
		return nil
	}

	ruleMap := ruleToMap(rule)
	ruleYAML, err := yaml.Marshal(ruleMap)
	if err != nil {
		return err
	}
	if mem.Scheme == url.Scheme(rule.Info.URL, "") {
		err = s.fs.Upload(ctx, rule.Info.URL, file.DefaultFileOsMode, bytes.NewReader(ruleYAML))
	}
	return err
}

func (s *service) initdBatch(request *build.Request, rule *config.Rule) {
	if request.Window > 0 {
		if rule.Batch == nil {
			rule.Batch = &config.Batch{Window: &config.Window{}}
		}
		rule.Batch.Window.DurationInSec = request.Window
		rule.Batch.Window.Init()
	}
}

func (s *service) buildQueryAction(query, destTable string) (*task.Action, error) {
	if destTable == "" {
		destTable = "mydataset.mytable"
	}
	destReference, err := base.NewTableReference(destTable)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid query dest table: %v", destTable)
	}
	return bq.NewQueryAction(query, destReference, "", true, nil), nil
}

func (s *service) initDestination(rule *config.Rule, request *build.Request) {
	rule.Dest.Table = request.Destination
	rule.Dest.Schema.Template = request.Template
	rule.Dest.Transient.Dataset = "temp"
	rule.Dest.Transient.ProjectID = request.ProjectID
	rule.Dest.Transient.Template = request.TransientTemplate
	rule.Dest.UniqueColumns = request.DedupeColumns
	if request.DestinationOverride {
		rule.Dest.Override = &request.DestinationOverride
	}
	if request.DestinationPartition {
		rule.Dest.Partition = config.DateExpr
	}
	if request.SourceFormat != "" {
		rule.Dest.SourceFormat = request.SourceFormat
	}
}

func (s *service) initSourceMatch(ctx context.Context, rule *config.Rule, request *build.Request) error {
	if request.MatchPattern == "" && (request.MatchPrefix == "" || request.MatchPrefix == shared.DefaultPrefix) && request.SourceURL != "" {
		objects, err := s.fs.List(ctx, request.SourceURL, foption.NewRecursive(true))
		if err != nil {
			return errors.Wrapf(err, "invalid source: %v", request.SourceURL)
		}
		folderCount := 0
		var extension = make(map[string]int)
		extensionMax := 0
		suffix := ""
		for _, object := range objects {
			if object.IsDir() {
				folderCount++
				continue
			}
			extension[path.Ext(object.Name())]++
			if extensionMax < extension[path.Ext(object.Name())] {
				suffix = path.Ext(object.Name())
				extensionMax = extension[path.Ext(object.Name())]
			}
		}
		objects, _ = s.fs.List(ctx, request.SourceURL)
		if len(objects) == 2 && objects[1].IsDir() {
			request.MatchPrefix = path.Join(request.MatchPrefix, objects[1].Name())
		}
		if suffix != "" {
			rule.When.Suffix = suffix
		}

		if folderCount > 1 && request.Window > 0 && rule.Batch == nil {
			rule.Batch = &config.Batch{Window: &config.Window{}}
			rule.Batch.MultiPath = true
		}
	}

	rule.When.Prefix = request.MatchPrefix
	rule.When.Suffix = request.MatchSuffix
	rule.When.Filter = request.MatchPattern
	if request.MatchPattern != "" {
		rule.Dest.Pattern = request.MatchPattern
	}
	return nil

}
