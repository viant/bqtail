package client

import (
	"context"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/bqtail/tail/config"
	"sync"
)

func (s *service) ingestDatafiles(ctx context.Context, waitGroup *sync.WaitGroup, object storage.Object, rule *config.Rule, response *TailResponse) {
	defer waitGroup.Done()
	if rule.HasMatch(object.URL()) {
		if err := s.publish(ctx, object, response); err != nil {
			response.AddError(err)
			s.Stop()

		}
		return
	}
	fmt.Printf("NO MATCHED: %v %+v\n", object.URL(), &rule.When)
	//TODO upload file
}
