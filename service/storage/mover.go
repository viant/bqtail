package storage

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"sync"
	"sync/atomic"
	"time"
)

type move struct {
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
	moves      chan *move
}

func (d *mover) move(ctx context.Context, sourceURL, destURL string) {
	defer d.Done()
	e := d.fs.Move(ctx, sourceURL, destURL)
	if e != nil {
		if exists, _ := d.fs.Exists(ctx, sourceURL, option.NewObjectKind(true)); exists {
			if atomic.CompareAndSwapInt32(&d.hasError, 0, 1) {
				d.errChannel <- e
			}
		}
		return
	}
}


func (d *mover) Schedule(schedule *move) {
	d.WaitGroup.Add(1)
	d.moves <- schedule
}

func (d *mover) Wait() (err error) {
	time.Sleep(100 * time.Millisecond)
	d.WaitGroup.Wait()
	atomic.StoreInt32(&d.closed, 1)
	for i := 0; i < d.routines; i++ {
		d.moves <- nil
	}
	if atomic.LoadInt32(&d.hasError) == 1 {
		err = <-d.errChannel
	}
	defer close(d.errChannel)
	defer close(d.moves)
	return err
}

func (d *mover) Run(ctx context.Context, routines int) {
	d.routines = routines
	d.moves = make(chan *move, routines)
	for i := 0; i < routines; i++ {
		d.WaitGroup.Add(1)
		go func() {
			d.WaitGroup.Done()
			for atomic.LoadInt32(&d.closed) == 0 {
				replay := <-d.moves
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
