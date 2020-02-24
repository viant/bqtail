package client

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/client/rule/build"
	"github.com/viant/bqtail/client/rule/validate"
	ctail "github.com/viant/bqtail/client/tail"
	"github.com/viant/bqtail/tail"
	"github.com/viant/bqtail/tail/contract"
	"sync/atomic"
)

//Service represents a client service
type Service interface {
	Build(ctx context.Context, request *build.Request) error

	Validate(ctx context.Context, request *validate.Request) error

	Load(ctx context.Context, request *ctail.Request) (*ctail.Response, error)

	Stop()
}

type service struct {
	config      *tail.Config
	tailService tail.Service
	fs          afs.Service
	stopped     int32

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

//New creates a service
func New() (Service, error) {
	ctx := context.Background()
	cfg, err := NewConfig(ctx, "")
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create scanFiles config")
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
