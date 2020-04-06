package cmd

import (
	"context"
	"github.com/jessevdk/go-flags"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/auth"
	"github.com/viant/bqtail/cmd/option"
	"github.com/viant/bqtail/cmd/rule/build"
	"github.com/viant/bqtail/cmd/rule/validate"
	"github.com/viant/bqtail/cmd/tail"
	"github.com/viant/bqtail/shared"
	"github.com/viant/toolbox"
	"log"
	"os"
)

const defaultOperationURL = "file:///tmp/bqtail/operation"

//RunClient run client
func RunClient(Version string, args []string) {
	options := &option.Options{}
	_, err := flags.ParseArgs(options, args)
	if isHelOption(args) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	client, err := auth.ClientFromURL(options.ClientURL())
	if err != nil {
		log.Fatal(err)
	}
	useGsUtilAuth := toolbox.AsBoolean(os.Getenv("GCLOUD_AUTH"))
	authService := auth.New(client, useGsUtilAuth, options.ProjectID, auth.Scopes...)
	setDefaultAuth(authService)

	if options.BaseOperationURL == "" {
		options.BaseOperationURL = defaultOperationURL
	}

	if options.Version {
		shared.LogF("BqTail: Version: %v\n", Version)
		return
	}

	if options.Logging != "" {
		os.Setenv(shared.LoggingEnvKey, options.Logging)
	}

	canBuildRule := options.Destination != ""
	canLoad := options.SourceURL != ""
	if !(canLoad || options.Validate || canBuildRule) && len(args) == 1 {
		os.Exit(1)
	}

	srv, err := New(options.ProjectID, options.BaseOperationURL)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if options.RuleURL == "" || canBuildRule {
		err = srv.Build(ctx, &build.Request{Options: options})
		if err != nil {
			log.Fatal(err)
		}
	}
	if options.Validate {
		err = srv.Validate(ctx, &validate.Request{Options: options})
		if err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}

	response, err := srv.Load(ctx, &tail.Request{options})
	if err != nil {
		log.Fatal(err)
	}
	shared.LogLn(response)
	if len(response.Errors) > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

func setDefaultAuth(authService auth.Service) {
	auth.DefaultHTTPClientProvider = authService.AuthHTTPClient
	auth.DefaultProjectProvider = authService.ProjectID
	gs.DefaultHTTPClientProvider = authService.AuthHTTPClient
	gs.DefaultProjectProvider = authService.ProjectID
}

func isHelOption(args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
