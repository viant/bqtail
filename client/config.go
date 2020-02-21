package client

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/tail"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
	"strings"
)

var async = false
var operationURL = "mem://localhost/operation"
var configURL = "mem://localhost/config"
var ruleURL = url.Join(configURL, "rule")

//NewConfig creates bqtail config
func NewConfig(ctx context.Context, projectID string) (*tail.Config, error) {
	credentials, err := google.FindDefaultCredentials(ctx, storage.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	if projectID == "" {
		projectID = credentials.ProjectID
	}
	cfg := &tail.Config{}
	cfg.Async = &async
	cfg.ProjectID = projectID
	cfg.ErrorURL = url.Join(operationURL, "errors")
	cfg.CorruptedFileURL = url.Join(operationURL, "corrupted")
	cfg.InvalidSchemaURL = url.Join(operationURL, "invalid_schema")
	cfg.SyncTaskURL = url.Join(operationURL, "tasks")
	cfg.AsyncTaskURL = url.Join(operationURL, "tasks")
	cfg.Ruleset.RulesURL = ruleURL
	cfg.MaxRetries = 3
	cfg.Ruleset.CheckInMs = 1
	cfg.URL = url.Join(configURL, "config.json")
	configJSON, _ := json.Marshal(cfg)
	fs := afs.New()
	if err := fs.Upload(ctx, cfg.URL, file.DefaultFileOsMode, bytes.NewReader(configJSON)); err != nil {
		return nil, errors.Wrapf(err, "failed to uplod conifg: %v", cfg.URL)
	}
	emptyRuleURL := url.Join(ruleURL, "t")
	_ = fs.Upload(ctx, emptyRuleURL, file.DefaultFileOsMode, strings.NewReader("."))
	err = cfg.Init(ctx, fs)
	return cfg, err
}
