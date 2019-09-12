package base

import (
	"context"
	"fmt"
	"golang.org/x/oauth2/google"
	"os"
)

const (
	cloudFunctionProjectEnvKey = "GCLOUD_PROJECT"
)

//Config represents base config
type Config struct {
	RunOnce      bool
	ProjectID    string
	DeferTaskURL string
	JournalURL   string
	ErrorURL     string
}

//OutputURL returns an output URL
func (c *Config) OutputURL(hasError bool) string {
	if hasError {
		return c.ErrorURL
	}
	return c.JournalURL
}

func (c *Config) Init(ctx context.Context) error {
	if c.ProjectID == "" {
		if project := os.Getenv(cloudFunctionProjectEnvKey); project != "" {
			c.ProjectID = project
			return nil
		}
		credentials, err := google.FindDefaultCredentials(ctx)
		if err != nil {
			return err
		}
		c.ProjectID = credentials.ProjectID
	}
	return nil
}

func (c *Config) Validate() error {
	if c.JournalURL == "" {
		return fmt.Errorf("journalURL was empty")
	}
	if c.ErrorURL == "" {
		return fmt.Errorf("errorURL was empty")
	}
	if c.DeferTaskURL == "" {
		return fmt.Errorf("eventURL were empty")
	}
	return nil
}
