package storage

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"sync"
	"sync/atomic"
)

type deleter struct {
	*sync.WaitGroup
	routines   int
	hasError   int32
	errChannel chan error
	closed     int32
	fs         afs.Service
	URLs       chan string
}

func (d *deleter) delete(ctx context.Context, URL string) {
	defer d.Done()
	if e := d.fs.Delete(ctx, URL, option.NewObjectKind(true)); e != nil {
		if ok, err := d.fs.Exists(ctx, URL, option.NewObjectKind(true)); !ok && err == nil {
			return
		}
		if atomic.CompareAndSwapInt32(&d.hasError, 0, 1) {
			d.errChannel <- e
		}
	}
}

func (d *deleter) Schedule(URL string) {
	d.WaitGroup.Add(1)
	d.URLs <- URL
}

func (d *deleter) Wait() (err error) {
	d.WaitGroup.Wait()
	atomic.StoreInt32(&d.closed, 1)
	for i := 0; i < d.routines; i++ {
		d.URLs <- ""
	}
	if atomic.LoadInt32(&d.hasError) == 1 {
		err = <-d.errChannel
	}
	defer close(d.errChannel)
	defer close(d.URLs)
	return err
}

func (d *deleter) Run(ctx context.Context, routines int) {
	d.routines = routines
	d.URLs = make(chan string, routines)
	for i := 0; i < routines; i++ {
		d.WaitGroup.Add(1)
		go func() {
			d.WaitGroup.Done()
			for atomic.LoadInt32(&d.closed) == 0 {
				URL := <-d.URLs
				if URL == "" {
					return
				}
				d.delete(ctx, URL)
			}
		}()
	}
}

func newDeleter(fs afs.Service) *deleter {
	return &deleter{
		WaitGroup:  &sync.WaitGroup{},
		errChannel: make(chan error, 1),
		fs:         fs,
	}
}
