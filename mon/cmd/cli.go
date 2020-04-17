package cmd

import (
	"context"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/viant/afsc/gs"
	"github.com/viant/bqtail/auth"
	"github.com/viant/bqtail/mon"
	"github.com/viant/bqtail/shared"
	"github.com/viant/toolbox"
	"log"
	"os"
)

//RunClient run client
func RunClient(Version string, args []string) {
	options := &Options{}
	_, err := flags.ParseArgs(options, args)
	if isHelOption(args) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	if options.Version {
		shared.LogF("BqTail: Version: %v\n", Version)
		return
	}

	options.Init()
	err = options.Validate()
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

	ctx := context.Background()
	service, err := mon.Singleton(ctx, options.ConfigURL)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "failed to create mon service with: %v", options.ConfigURL))
	}
	response := service.Check(ctx, &mon.Request{
		IncludeDone: options.IncludeDone,
		Recency:     options.Recency,
	})
	toolbox.DumpIndent(response, true)
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
