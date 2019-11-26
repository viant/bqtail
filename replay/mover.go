package replay

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"sync"
	"sync/atomic"
)

type replay struct {
	src  string
	dest string
}

type mover struct {
	*sync.WaitGroup
	routines   int
	hasError   int32
	errChannel chan error
	closed     int32
	fs         afs.Service
	replays    chan *replay
}

func (d *mover) move(ctx context.Context, sourceURL, destURL string) {
	defer d.Done()
	e := d.fs.Move(ctx, sourceURL, destURL);
	if e != nil {
		if atomic.CompareAndSwapInt32(&d.hasError, 0, 1) {
			d.errChannel <- e
		}
		fmt.Printf("err: %v\n", e)
		return
	}

	if e := d.fs.Move(ctx, destURL, sourceURL); e != nil {
		if atomic.CompareAndSwapInt32(&d.hasError, 0, 1) {
			d.errChannel <- e
		}
		fmt.Printf("err: %v\n", e)
	}
	fmt.Printf("replayed: %v\n", sourceURL)
}

func (d *mover) Schedule(reply *replay) {
	d.WaitGroup.Add(1)
	d.replays <- reply
}

func (d *mover) Wait() (err error) {
	d.WaitGroup.Wait()
	atomic.StoreInt32(&d.closed, 1)
	for i := 0; i < d.routines; i++ {
		d.replays <- nil
	}
	if atomic.LoadInt32(&d.hasError) == 1 {
		err = <-d.errChannel
	}
	defer close(d.errChannel)
	defer close(d.replays)
	return err
}

func (d *mover) Run(ctx context.Context, routines int) {
	d.routines = routines
	d.replays = make(chan *replay, routines)
	for i := 0; i < routines; i++ {
		d.WaitGroup.Add(1)
		go func() {
			d.WaitGroup.Done()
			for atomic.LoadInt32(&d.closed) == 0 {
				replay := <-d.replays
				if replay == nil {
					return
				}
				d.move(ctx, replay.src, replay.dest)
			}
		}()
	}
}

func newMover(fs afs.Service) *mover {
	return &mover{
		WaitGroup:  &sync.WaitGroup{},
		errChannel: make(chan error, 1),
		fs:         fs,
	}
}
