package load

import (
	"github.com/viant/bqtail/base"
	"github.com/viant/bqtail/service/bq"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/batch"
	"github.com/viant/bqtail/tail/config"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"google.golang.org/api/bigquery/v2"
	"os"
	"path"
	"testing"
)

func TestJob_NewLoadRequest(t *testing.T) {


	os.Setenv(base.LoggingEnvKey, "true")


	baseURL := path.Join(toolbox.CallerDirectory(3), "test")


	var useCases = []struct {
		description  string
		caseURL string
		rule         config.Rule
		process      stage.Process
		window       batch.Window
		expect       interface{}
		tables       map[string]*bigquery.Table
		hasInitError bool
	}{
		{
			description: "single sync ingestion request",
			caseURL:path.Join(baseURL , "001_single_sync"),

		},
		{
			description: "batch sync ingestion request",
			caseURL:path.Join(baseURL , "002_batch_sync"),
		},
		{
			description: "transient sync ingestion request",
			caseURL:path.Join(baseURL , "003_transient_sync"),
		},
		{
			description: "transient dedupe ingestion request",
			caseURL:path.Join(baseURL , "004_dedupe_async"),
		},
	}

	for _, useCase := range useCases[3:] {
		ctx := context.Background()

		err := loadTestAsset(ctx, &useCase.process,path.Join(useCase.caseURL, "process.json"))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		err = loadTestAsset(ctx, &useCase.rule,path.Join(useCase.caseURL, "rule.json"))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		err = loadTestAsset(ctx, &useCase.window,path.Join(useCase.caseURL, "window.json"))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		useCase.tables = make(map[string]*bigquery.Table)
		err = loadTestAsset(ctx, &useCase.tables,path.Join(useCase.caseURL, "tables.json"))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		var window *batch.Window
		if useCase.rule.Batch != nil {
			window = &useCase.window
		}
		job, err := NewJob(&useCase.rule, &useCase.process, window)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		bqService := bq.NewFakerWithTables(useCase.tables)
		initErr := job.Init(context.Background(), bqService)
		if useCase.hasInitError {
			assert.NotNil(t, initErr, useCase.description)
			continue
		}
		if !assert.Nil(t, initErr, useCase.description) {
			fmt.Println(initErr)
			continue
		}


		useCase.expect = map[string]interface{}{}
		err = loadTestAsset(ctx, &useCase.expect,path.Join(useCase.caseURL, "expect.json"))
		if !assert.Nil(t, err, useCase.description) {
			continue
		}

		_, action := job.NewLoadRequest()
		if ! assertly.AssertValues(t, useCase.expect, action, useCase.description) {
			toolbox.DumpIndent(action, true)
		}
	}
}



func loadTestAsset(ctx context.Context, asset interface{}, location string) error {
	fs := afs.New()
	if exists, _ := fs.Exists(ctx, location); !exists {
		return nil
	}
	reader, err := fs.DownloadWithURL(ctx, location)
	if err != nil {
		return err
	}
	defer reader.Close()
	return json.NewDecoder(reader).Decode(asset)
}
