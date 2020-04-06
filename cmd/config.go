package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/auth"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail"
	"strings"
)

var async = false
var operationURL = url.Join(shared.InMemoryStorageBaseURL, "operation")
var configURL = url.Join(shared.InMemoryStorageBaseURL, "/BqTail/config/")
var ruleBaseURL = url.Join(configURL, "rule")

//NewConfig creates bqtail config
func NewConfig(ctx context.Context, projectID string, baseOpsURL string) (*tail.Config, error) {
	cfg, err := newConfig(ctx, projectID, baseOpsURL)
	if err != nil {
		return cfg, err
	}
	configJSON, _ := json.Marshal(cfg)
	fs := afs.New()
	if err := fs.Upload(ctx, cfg.URL, file.DefaultFileOsMode, bytes.NewReader(configJSON)); err != nil {
		return nil, errors.Wrapf(err, "failed to upload config: %v", cfg.URL)
	}
	emptyRuleURL := url.Join(ruleBaseURL, "t")
	_ = fs.Upload(ctx, emptyRuleURL, file.DefaultFileOsMode, strings.NewReader("."))
	err = cfg.Init(ctx, fs)
	return cfg, err
}

func newConfig(ctx context.Context, projectID, baseOpsURL string) (*tail.Config, error) {
	var err error
	if projectID == "" {
		if projectID, err = auth.DefaultProjectProvider(ctx, auth.Scopes); err != nil {
			return nil, err
		}
	}
	cfg := &tail.Config{}
	cfg.Async = &async
	cfg.ProjectID = projectID

	if baseOpsURL == "" {
		baseOpsURL = operationURL
	}
	cfg.ErrorURL = url.Join(baseOpsURL, "errors")
	cfg.CorruptedFileURL = url.Join(baseOpsURL, "corrupted")
	cfg.InvalidSchemaURL = url.Join(baseOpsURL, "invalid_schema")
	cfg.JournalURL = url.Join(baseOpsURL, "journal")
	cfg.SyncTaskURL = url.Join(operationURL, "tasks")
	cfg.AsyncTaskURL = url.Join(operationURL, "tasks")
	cfg.Ruleset.RulesURL = ruleBaseURL
	cfg.MaxRetries = 3
	cfg.Ruleset.CheckInMs = 1
	cfg.URL = url.Join(configURL, "config.json")
	return cfg, err
}
