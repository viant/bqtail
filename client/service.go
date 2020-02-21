package client

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/bqtail/tail"
	"github.com/viant/bqtail/tail/config"
	"github.com/viant/bqtail/tail/contract"
	"sync/atomic"
)

//Service represents a client service
type Service interface {
	BuildRule(ctx context.Context, request *BuildRuleRequest) (*config.Rule, error)

	ValidateRule(ctx context.Context, request *ValidateRuleRequest) error

	Tail(ctx context.Context, request *TailRequest) (*TailResponse, error)

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
		return nil, errors.Wrapf(err, "failed to create tail config")
	}
	tailService, err := tail.New(ctx, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create tail service")
	}
	return &service{
		config:       cfg,
		fs:           afs.New(),
		tailService:  tailService,
		requestChan:  make(chan *contract.Request, processingChannelSize),
		responseChan: make(chan *contract.Response, processingChannelSize),
		stopChan:     make(chan bool, 2),
	}, nil
}
