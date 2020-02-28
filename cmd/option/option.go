package option

import (
	"crypto/md5"
	"encoding/base64"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/tail"
	"os"
	"path"
	"strings"
)

type Options struct {
	RuleURL string `short:"r" long:"rule" description:"rule URL"`

	Validate bool `short:"V" long:"validate" description:"run validation"`

	Version bool `short:"v" long:"version" description:"bqtail version"`

	ProjectID string `short:"p" long:"project" description:"Google Cloud Project"`

	Bucket string `short:"b" long:"bucket" description:"Google Cloud Storage ingestion bucket"`

	Destination string `short:"d" long:"dest" description:"destination table" `

	DestinationPartition bool `short:"n" long:"dest-partition" description:"destination partition"`

	DestinationOverride bool `short:"o" long:"dest-override" description:"override destination"`

	Template string `short:"t" long:"template" description:"table template"`

	TransientTemplate string `short:"T" long:"transient-template" description:"table template"`

	DedupeColumns []string `short:"D" long:"dedupe-columns" description:"deduplication columns"`

	MatchPrefix string `short:"P" long:"prefix" description:"source match prefix"`

	MatchSuffix string `short:"S" long:"suffix" description:"source match suffix"`

	MatchPattern string `short:"R" long:"reg expr pattern" description:"source match reg expr pattern"`

	SourceURL string `short:"s" long:"src" description:"source data URL" `

	SourceFormat string `short:"f" long:"source-format" description:"load job format"  choice:"CSV" choice:"NEWLINE_DELIMITED_JSON" choice:"AVRO" choice:"PARQUET"`

	Window int `short:"w" long:"window" description:"batching window in sec"`

	Logging string `short:"l" long:"logging" description:"logging level" choice:"info" choice:"debug" choice:"off" default:"info" `

	HistoryURL string `short:"H" long:"history" description:"history url to track already process file in previous run"`

	Stream bool `short:"X" long:"stream" description:"run constantly to stream changed/new datafile(s)"`

	Client string `short:"c" long:"client" description:"GCP OAuth client url"`
}

//ClientURI returns clientURL
func (r *Options) ClientURL() string {
	if r.Client == "" {
		r.Client = shared.ClientSecretURL
	}
	return r.Client
}

//HistoryPathURL return history URL
func (r *Options) HistoryPathURL(URL string) string {
	urlPath := url.Path(URL)
	historyName := md5Hash(urlPath) + ".json"
	historyName = strings.Replace(historyName, "=", "", strings.Count(historyName, "="))
	return url.Join(r.HistoryURL, historyName)
}

//Hash returns fnv fnvHash value
func md5Hash(key string) string {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	data := h.Sum(nil)
	return base64.StdEncoding.EncodeToString(data)
}

func (r *Options) Init(config *tail.Config) {
	if r.MatchPattern == "" && r.MatchPrefix == "" {
		r.MatchPrefix = shared.DefaultPrefix
	}
	if r.SourceURL != "" {
		r.SourceURL = normalizeLocation(r.SourceURL)
	}
	if r.RuleURL != "" {
		r.RuleURL = normalizeLocation(r.RuleURL)
	}

	if r.HistoryURL != "" {
		r.HistoryURL = normalizeLocation(r.HistoryURL)
	}

	if r.Bucket == "" {
		r.Bucket = config.TriggerBucket
	}
	r.initHistoryURL()
}

func (r *Options) initHistoryURL() {
	if r.HistoryURL != "" {
		return
	}
	r.HistoryURL = path.Join(os.Getenv("HOME"), ".bqtail")
	if !r.Stream {
		r.HistoryURL = url.Join(shared.InMemoryStorageBaseURL, r.HistoryURL)
	}
}

func normalizeLocation(location string) string {
	if strings.HasPrefix(location, "~/") {
		location = strings.Replace(location, "~/", os.Getenv("HOME"), 1)
	}
	if url.Scheme(location, "") == "" && !strings.HasPrefix(location, "/") {
		currentDirectory, _ := os.Getwd()
		return path.Join(currentDirectory, location)
	}
	return location
}
