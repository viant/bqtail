package tail

import (
	"context"
	"fmt"
	"bqtail/tail/contract"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path"
	"testing"
	"time"
)

func TestService_Tail(t *testing.T) {

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/gcp-e2e.json"))
	parent := toolbox.CallerDirectory(3)

	configURL := path.Join(parent, "../e2e/config/bqtail.json")
	ctx := context.Background()
	config, err := NewConfigFromURL(ctx, configURL)
	if err != nil {
		log.Fatal(err)
	}
	srv, err := New(ctx, config)
	if err != nil {
		log.Fatal(err)
	}

	index := 1
	eventID := fmt.Sprintf("e%v", time.Now().UnixNano())

	response := srv.Tail(ctx, &contract.Request{
		EventID:   eventID,
		SourceURL: fmt.Sprintf("gs://e2e-data/data/case005/dummy%d.json", index),
	})
	toolbox.Dump(response)

}
