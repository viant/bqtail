package base

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/bqtail/auth"
	"github.com/viant/bqtail/shared"
	"github.com/viant/bqtail/stage"
	"github.com/viant/bqtail/stage/activity"
	"os"
	"path"
	"strings"
	"time"
)

const (
	defaultRegion = "us-central1"
)

var cloudFunctionProjectEnvKeys = []string{"GCLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT", "GCP_PROJECT"}
var cloudFunctionRegionEnvKeys = []string{"FUNCTION_REGION", "GOOGLE_CLOUD_REGION"}

//Config represents base config
type Config struct {
	URL                  string
	RunOnce              bool
	ProjectID            string
	Region               string
	AsyncTaskURL         string
	SyncTaskURL          string
	ActiveLoadProcessURL string
	DoneLoadProcessURL   string
	JournalURL           string
	TriggerBucket        string
	LoadProcessPrefix    string
	PostJobPrefix        string
	BatchPrefix          string
	BqJobInfoPath        string
	BqBatchInfoPath      string

	ErrorURL         string
	CorruptedFileURL string
	InvalidSchemaURL string
	SlackCredentials *Secret
	MaxRetries       int
}

//BuildLoadURL returns active action ProcessURL for supplied event id
func (c *Config) BuildLoadURL(info *stage.Process) string {
	return url.Join(c.ActiveLoadProcessURL, info.DestTable+shared.PathElementSeparator+info.EventID+shared.ProcessExt)
}

//DoneLoadURL returns done action ProcessURL for supplied event id
func (c *Config) DoneLoadURL(info *stage.Process) string {
	date := time.Now().Format(shared.DateLayout)
	return url.Join(c.DoneLoadProcessURL, path.Join(info.DestTable, date, info.EventID+shared.ProcessExt))
}

//BuildTaskURL returns an action url for supplied event ID
func (c *Config) BuildTaskURL(info *activity.Meta) string {
	date := time.Now().Format(shared.DateLayout)
	return fmt.Sprintf("gs://%v%v%v/%v", c.TriggerBucket, c.PostJobPrefix, date, info.JobFilename())
}

//Init initialises config
func (c *Config) Init(ctx context.Context) error {
	if c.ProjectID == "" {
		for _, key := range cloudFunctionProjectEnvKeys {
			if project := os.Getenv(key); project != "" {
				c.ProjectID = project
				break
			}
		}
		if c.ProjectID == "" {
			project, err := auth.DefaultProjectProvider(ctx, auth.Scopes)
			if err != nil {
				return err
			}
			c.ProjectID = project
		}
	}

	if c.TriggerBucket == "" {
		c.TriggerBucket = strings.Replace(c.ProjectID, "-", "_", len(c.ProjectID)) + "_bqtail"
	}

	if c.Region == "" {
		for _, key := range cloudFunctionRegionEnvKeys {
			if region := os.Getenv(key); region != "" {
				c.Region = region
				break
			}
		}
	}
	if c.Region == "" {
		c.Region = defaultRegion
	}

	if c.LoadProcessPrefix == "" {
		c.LoadProcessPrefix = shared.LoadPrefix
	}
	if c.PostJobPrefix == "" {
		c.PostJobPrefix = shared.PostJobPrefix
	}
	if c.BatchPrefix == "" {
		c.BatchPrefix = shared.BatchPrefix
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = shared.MaxRetries
	}
	if c.ActiveLoadProcessURL == "" {
		c.ActiveLoadProcessURL = url.Join(c.JournalURL, shared.ActiveLoadSuffix)
	}
	if c.DoneLoadProcessURL == "" {
		c.DoneLoadProcessURL = url.Join(c.JournalURL, shared.DoneLoadSuffix)
	}
	if c.InvalidSchemaURL == "" {
		c.InvalidSchemaURL = url.Join(c.JournalURL, shared.InvalidSchemaLocation)
	}
	return nil
}

//Validate checks if config is valid
func (c *Config) Validate() error {
	if c.JournalURL == "" {
		return fmt.Errorf("journalURL was empty")
	}
	if c.ErrorURL == "" {
		return fmt.Errorf("errorURL was empty")
	}
	if c.AsyncTaskURL == "" {
		return fmt.Errorf("asyncTaskURL were empty")
	}
	if c.TriggerBucket == "" {
		return fmt.Errorf("triggerBucket were empty")
	}

	return nil
}
