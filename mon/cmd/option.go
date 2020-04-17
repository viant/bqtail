package cmd

import (
	"github.com/pkg/errors"
	"github.com/viant/bqtail/shared"
)

type Options struct {
	ConfigURL   string `short:"c" long:"cfg" description:"Serverless BqTail config URL"`
	IncludeDone bool   `short:"i" long:"idone" description:"include done process"`
	Recency     string `short:"r" long:"recency" description:"recency expression"`
	ProjectID   string `short:"p" long:"project" description:"Google Cloud Project"`
	Client      string `short:"a" long:"aclient" description:"GCP OAuth client url"`
	Version     bool   `short:"v" long:"version" description:"bqtail version"`
}

//Init initialises options
func (o *Options) Init() {
	if o.Recency == "" {
		o.Recency = "1hour"
	}
}

func (o *Options) Validate() error {
	if o.ConfigURL == "" {
		return errors.Errorf("configURL was empty")
	}
	return nil
}

//ClientURI returns clientURL
func (o *Options) ClientURL() string {
	if o.Client == "" {
		o.Client = shared.ClientSecretURL
	}
	return o.Client
}
