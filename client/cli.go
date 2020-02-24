package client

import (
	"context"
	"github.com/jessevdk/go-flags"
	"github.com/viant/bqtail/client/option"
	"github.com/viant/bqtail/client/rule/build"
	"github.com/viant/bqtail/client/rule/validate"
	"github.com/viant/bqtail/client/tail"
	"github.com/viant/bqtail/shared"
	"github.com/viant/toolbox"
	"log"
	"os"
)

//RunClient run client
func RunClient(args []string) {
	options := &option.Options{}
	_, err := flags.ParseArgs(options, args)
	if isHelOption(args) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	if options.Logging != "" {
		os.Setenv(shared.LoggingEnvKey, options.Logging)
	}
	toolbox.Dump(options)
	canBuildRule := options.Destination != ""
	canLoad := options.SourceURL != ""
	if !(canLoad || options.Validate || canBuildRule) && len(args) == 1 {
		os.Exit(1)
	}

	srv, err := New()
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

func isHelOption(args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
