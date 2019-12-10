package base

import (
	"bqtail/stage"
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"golang.org/x/oauth2/google"
	"os"
	"path"
	"time"
)

const (
	defaultRegion = "us-central1"
)

var cloudFunctionProjectEnvKeys = []string{"GCLOUD_PROJECT", "GOOGLE_CLOUD_PROJECT"}
var cloudFunctionRegionEnvKeys = []string{"FUNCTION_REGION", "GOOGLE_CLOUD_REGION"}

//Config represents base config
type Config struct {
	URL              string
	RunOnce          bool
	ProjectID        string
	Region           string
	AsyncTaskURL     string
	SyncTaskURL      string
	ActiveLoadJobURL string
	DoneLoadJobURL   string
	JournalURL       string
	TriggerBucket    string
	LoadJobPrefix    string
	BqJobPrefix      string
	BatchPrefix      string
	ErrorURL         string
	CorruptedFileURL string
	InvalidSchemaURL string
	SlackCredentials *Secret
}

//BuildActiveLoadURL returns active action URL for supplied event id
func (c *Config) BuildActiveLoadURL(info *stage.Info) string {
	return url.Join(c.ActiveLoadJobURL, info.DestTable+stage.PathElementSeparator+info.EventID+ActionExt)
}

//BuildDoneLoadURL returns done action URL for supplied event id
func (c *Config) BuildDoneLoadURL(info *stage.Info) string {
	date := time.Now().Format(DateLayout)
	return url.Join(c.DoneLoadJobURL, path.Join(info.DestTable, date, info.EventID+ActionExt))
}

//BuildTaskURL returns an action url for supplied event ID
func (c *Config) BuildTaskURL(info *stage.Info) string {
	date := time.Now().Format(DateLayout)
	return fmt.Sprintf("gs://%v%v%v/%v%v", c.TriggerBucket, c.BqJobPrefix, date, info.ID(), JobExt)
}

//OutputURL returns an output URL
func (c *Config) OutputURL(hasError bool) string {
	if hasError {
		return c.ErrorURL
	}
	return c.JournalURL
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
			credentials, err := google.FindDefaultCredentials(ctx)
			if err != nil {
				return err
			}
			c.ProjectID = credentials.ProjectID
		}
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

	if c.LoadJobPrefix == "" {
		c.LoadJobPrefix = LoadPrefix
	}
	if c.BqJobPrefix == "" {
		c.BqJobPrefix = BqJobPrefix
	}
	if c.BatchPrefix == "" {
		c.BatchPrefix = BatchPrefix
	}
	if c.ActiveLoadJobURL == "" {
		c.ActiveLoadJobURL = url.Join(c.JournalURL, ActiveLoadSuffix)
	}
	if c.DoneLoadJobURL == "" {
		c.DoneLoadJobURL = url.Join(c.JournalURL, DoneLoadSuffix)
	}
	if c.InvalidSchemaURL == "" {
		c.InvalidSchemaURL = url.Join(c.JournalURL, InvalidSchemaLocation)
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
