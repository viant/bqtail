package base

import (
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
	RunOnce          bool
	ProjectID        string
	Region           string
	DeferTaskURL     string
	BatchURL         string
	JournalURL       string
	TriggerBucket    string
	ActionPrefix     string
	ErrorURL         string
	SlackCredentials *Secret
}

//BuildReplayActionURL returns replay action URL for supplied event id
func (c *Config) BuildReplayActionURL(eventID string) string {
	date := time.Now().Format(DateLayout)
	return url.Join(c.JournalURL, path.Join(replayPrefix, date, eventID+ActionExt))
}

//BuildActionURL returns an action url for supplied event ID
func (c *Config) BuildActionURL(eventID string) string {
	date := time.Now().Format(DateLayout)
	return fmt.Sprintf("gs://%v%v%v/%v%v", c.TriggerBucket, c.ActionPrefix, date,DecodePathSeparator(eventID), ActionExt)
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

	if c.ActionPrefix == "" {
		c.ActionPrefix = actionPrefix
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
	if c.DeferTaskURL == "" {
		return fmt.Errorf("deferTaskURL were empty")
	}
	if c.TriggerBucket == "" {
		return fmt.Errorf("triggerBucket were empty")
	}

	return nil
}
