package batch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/config/pattern"
)

func TestService_MatchWindowDataURLs_GroupByParameters(t *testing.T) {
	ctx := context.Background()
	fs := afs.New()
	baseURL := "mem://localhost/grouped-data/"
	URLs := []string{
		baseURL + "ip_123_a.json",
		baseURL + "ip_123_b.json",
		baseURL + "device_234_a.json",
		baseURL + "device_234_b.json",
	}
	for _, URL := range URLs {
		require.NoError(t, fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader("{}")))
	}

	rule := newGroupedBatchRule("/grouped-data/", []string{"UidType", "UserPoolID"})
	rule.Dest.Table = "dataset.table_$Mod(50)"
	now := time.Now()
	process := stage.NewProcess("event-1", stage.NewSource(URLs[0], now), "rule.yaml", false)
	var err error
	process.Params, err = rule.Dest.Params(process.Source.URL)
	require.NoError(t, err)
	groupKey, err := rule.Batch.GroupKey(process.Params)
	require.NoError(t, err)
	process.DestTable, err = rule.Dest.ExpandTableWithModSource(rule.Dest.Table, process.Source, groupKey)
	require.NoError(t, err)
	window := NewWindow(process, now.Add(-time.Minute), now.Add(time.Minute), "mem://localhost/batch-group-test/window.win")

	service := New(func(*config.Rule) string { return "mem://localhost/batch-group-test/tasks" }, fs)
	require.NoError(t, service.MatchWindowDataURLs(ctx, rule, window))
	assert.ElementsMatch(t, URLs[:2], window.URIs)
}

func TestService_MatchWindowDataURLs_GroupingDisabled(t *testing.T) {
	ctx := context.Background()
	fs := afs.New()
	baseURL := "mem://localhost/ungrouped-data/"
	URLs := []string{
		baseURL + "ip_123.json",
		baseURL + "device_234.json",
	}
	for _, URL := range URLs {
		require.NoError(t, fs.Upload(ctx, URL, file.DefaultFileOsMode, strings.NewReader("{}")))
	}

	rule := newGroupedBatchRule("/ungrouped-data/", nil)
	now := time.Now()
	process := stage.NewProcess("event-1", stage.NewSource(URLs[0], now), "rule.yaml", false)
	process.DestTable = rule.Dest.Table
	process.Params, _ = rule.Dest.Params(process.Source.URL)
	window := NewWindow(process, now.Add(-time.Minute), now.Add(time.Minute), "mem://localhost/batch-group-disabled-test/window.win")

	service := New(func(*config.Rule) string { return "mem://localhost/batch-group-disabled-test/tasks" }, fs)
	require.NoError(t, service.MatchWindowDataURLs(ctx, rule, window))
	assert.ElementsMatch(t, URLs, window.URIs)
}

func newGroupedBatchRule(prefix string, groupBy []string) *config.Rule {
	return &config.Rule{
		When: matcher.Basic{Prefix: prefix, Suffix: ".json"},
		Dest: &config.Destination{
			Table:   "dataset.table",
			Pattern: prefix + `(ip|device)_(\d+)`,
			Parameters: []*pattern.Param{
				{Name: "UidType", Expression: "$1"},
				{Name: "UserPoolID", Expression: "$2"},
			},
		},
		Batch: &config.Batch{GroupByParameters: groupBy},
	}
}
