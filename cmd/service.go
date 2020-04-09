package cmd

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/cmd/rule/build"
	"github.com/viant/bqtail/cmd/rule/validate"
	ctail "github.com/viant/bqtail/cmd/tail"
	"github.com/viant/bqtail/tail"
	"github.com/viant/bqtail/tail/contract"
	"sync/atomic"
)

//Service represents a client service
type Service interface {
	//Build build a rule for cli options
	Build(ctx context.Context, request *build.Request) error
	//Validate check rule either build or with specified URL
	Validate(ctx context.Context, request *validate.Request) error
	//Load start load process for specified source and rule
	Load(ctx context.Context, request *ctail.Request) (*ctail.Response, error)
	//Stop stop service
	Stop()
}

type service struct {
	config       *tail.Config
	tailService  tail.Service
	fs           afs.Service
	stopped      int32
	stopChan     chan bool
	requestChan  chan *contract.Request
	responseChan chan *contract.Response
}

func (s *service) Stop() {
	if atomic.CompareAndSwapInt32(&s.stopped, 0, 1) {
		for i := 0; i < 2; i++ {
			s.stopChan <- true
		}
	}
}

func (s *service) reportSettings(request *ctail.Request, config *tail.Config) {
	fmt.Printf("==== SETTINGS ====\n")
	fmt.Printf("GCP Project: '%v'\n", config.ProjectID)
	fmt.Printf("GCS Bucket: '%v'\n", request.Bucket)
	fmt.Printf("Operations URL: '%v'\n", request.BaseOperationURL)
}

//New creates a service
func New(projectID string, baseOpsURL string) (Service, error) {
	ctx := context.Background()
	cfg, err := NewConfig(ctx, projectID, baseOpsURL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create config")
	}
	tailService, err := tail.New(ctx, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create scanFiles service")
	}
	return &service{
		config:       cfg,
		fs:           afs.New(),
		tailService:  tailService,
		requestChan:  make(chan *contract.Request, processingRoutines),
		responseChan: make(chan *contract.Response, processingRoutines),
		stopChan:     make(chan bool, 2),
	}, nil
}
