package uploader

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"sync"
	"sync/atomic"
	"time"
)

//Service represent uploader service
type Service interface {
	Schedule(request *Request)
	Wait() error
}

type service struct {
	*sync.WaitGroup
	routines   int
	hasError   int32
	errChannel chan error
	OnDone
	closed  int32
	fs      afs.Service
	uploads chan *Request
}

func (d *service) upload(ctx context.Context, upload *Request) {
	defer d.Done()
	e := d.fs.Copy(ctx, upload.src, upload.dest)
	if e != nil {
		e = errors.Wrapf(e, "failed to copy %v to %v", upload.src, upload.dest)
	}
	if d.OnDone != nil {
		d.OnDone(upload.dest, e)
	}
	if e != nil {
		if atomic.CompareAndSwapInt32(&d.hasError, 0, 1) {
			d.errChannel <- e
		}
		return
	}

}

func (d *service) Schedule(request *Request) {
	d.WaitGroup.Add(1)
	d.uploads <- request
}

func (d *service) Wait() (err error) {
	time.Sleep(1 * time.Second)
	d.WaitGroup.Wait()
	atomic.StoreInt32(&d.closed, 1)
	for i := 0; i < d.routines; i++ {
		d.uploads <- nil
	}
	if atomic.LoadInt32(&d.hasError) == 1 {
		err = <-d.errChannel
	}
	defer close(d.errChannel)
	defer close(d.uploads)
	return err
}

func (d *service) init(ctx context.Context, routines int) {
	d.routines = routines
	d.uploads = make(chan *Request, routines)
	for i := 0; i < routines; i++ {
		d.WaitGroup.Add(1)
		go func() {
			d.WaitGroup.Done()
			for atomic.LoadInt32(&d.closed) == 0 {
				upload := <-d.uploads
				if upload == nil {
					return
				}
				d.upload(ctx, upload)
			}
		}()
	}
}

//New creates a upload service
func New(ctx context.Context, fs afs.Service, done OnDone, routines int) Service {
	srv := &service{
		OnDone:     done,
		WaitGroup:  &sync.WaitGroup{},
		errChannel: make(chan error, 1),
		fs:         fs,
	}
	srv.init(ctx, routines)
	return srv
}
