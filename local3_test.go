package bqtail

import (
	"bqtail/base"
	"bqtail/dispatch"
	"context"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
)

func TestDispatch3(t *testing.T) {



	os.Setenv("GCLOUD_PROJECT", "viant-dataflow")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/viant-dataflow.json"))
	os.Setenv(base.ConfigEnvKey, "gs://viant_dataflow_config/BqDispatch/config.json")
	os.Setenv(base.LoggingEnvKey, "true")

	ctx := context.Background()
	service, err := dispatch.Singleton(ctx)
	if err != nil {
		return
	}
	response := service.Dispatch(ctx)
	toolbox.Dump(response)
}

