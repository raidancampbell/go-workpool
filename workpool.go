// package go_workpool implements a workpool synchronized on a work item's Key.
//in the course of the workpool's life, two times the number of unique keys can be created
//one goroutine per key max for parallel processing
//another goroutine per key max for work queue management
package go_workpool

import (
	"context"
	xsync "golang.org/x/sync/semaphore"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Work is the interface for callers to use this library.  Each unit of work (such as an event) must implement the Work interface
type Work interface {
	// Key should return a value that identifies what the work is being performed on
	//For example:
	//If account "a" has a creation event, followed by an update, then a cancellation event
	//All three events should return "a".  This will cause the creation event to process, and the update/cancellation events to queue
	Key() string

	// Do should perform the actual work required.  Do is called in its own goroutine
	Do()
}

// Workpool
type Workpool struct {
	// how much work is there in total.  This is just for cute metrics or whatever.  Not much real value in this
	queueLen *uint64

	submitMtx sync.Mutex
	// the actual pool of work.  Indexed by key, each value is a queue of work for that key
	pool *sync.Map

	// a mutex for each key, to notify when new work is ready
	notif *sync.Map

	// when there's no work, this needs to block with a non-busy method.
	// When work is added, this needs to pass through
	noWork *sync.Map

	// goroutines will die after all their work is done and be recreated when more work arrives for them
	isAlive *sync.Map
}

type workQueue struct {
	// queue of work
	mtx *sync.Mutex
	queue []Work
}

func (wq *workQueue) enqueue(w Work) {
	wq.mtx.Lock()
	defer wq.mtx.Unlock()
	wq.queue = append(wq.queue, w)
}

func (wq *workQueue) deque() Work {
	wq.mtx.Lock()
	defer func() {
		wq.queue = wq.queue[1:]
		wq.mtx.Unlock()
	}()
	return wq.queue[0]
}

func New() *Workpool {
	return &Workpool{
		queueLen: new(uint64),
		pool:     &sync.Map{},
		notif:    &sync.Map{},
		noWork:   &sync.Map{},
		isAlive: &sync.Map{},
	}
}

// manages the work queue for a given key
//At max, there will be N active goroutines of manageKeyQueue, where N is the number of unique keys
func (wp *Workpool) manageKeyQueue(key string) {
	for {
		// lock this key's work. just make sure any earlier work on this key is already done
		notif, _ := wp.notif.Load(key)
		notif.(*sync.Mutex).Lock()

		// wait 100 ms for any work.  If none comes, die
		nw, _ := wp.noWork.Load(key)
		ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(100 * time.Millisecond))
		// there's a race between failing to find work and someone giving us work.
		// the below solution makes the race benign by allowing another copy of this goroutine to be created
		// the timeouts allow the issue to heal itself.
		err := nw.(*xsync.Weighted).Acquire(ctx, 1)
		if err != nil {
			// mark myself as offline.  Any raced copies of this function are still blocked by the mutex
			wp.isAlive.Store(key, false)
			// Do another check.  if there's really no work, then quit.  The second 100ms is a "best effort" synchronization
			// this allows the Submit function an extra 100ms to spin up a raced copy of this goroutine.
			// any raced copies of this function are still blocked by the mutex.
			err := nw.(*xsync.Weighted).Acquire(ctx, 1)
			if err != nil {
				// final point of race: if a piece of work is submitted now, we won't execute it.
				//another raced groutine will have to take it
				// free up the mutex for any raced goroutine
				notif.(*sync.Mutex).Unlock()
				return
			}
		}
		// grab the work, since we know some is ready
		p, _ := wp.pool.Load(key)
		work := p.(*workQueue).deque()

		// fork off to complete the work.  After the work is completed, unlock the mutex
		go func() {
			work.Do()
			atomic.AddUint64(wp.queueLen, ^uint64(0))
			notif.(*sync.Mutex).Unlock()
		}()

		// if we timed out earlier, there's another copy of our goroutine alive
		// we already marked ourselves as dead.  let the raced copy of our goroutine take over once the work is done
		// and the mutex is released
		if err != nil {
			return
		}
	}
}

// Submit submits the given work to the workpool.  If other work is already in place with the same key, then this work
// will be queued.  Order is guaranteed as a FIFO queue.
func (wp *Workpool) Submit(w Work) {
	wp.submitMtx.Lock()
	defer wp.submitMtx.Unlock()

	// the notif map is recycled to indicate whether the key has ever been seen before
	if _, ok := wp.notif.Load(w.Key()); !ok {
		// if this is the first time we've seen this key, set everything up
		wp.pool.Store(w.Key(), &workQueue{queue: make([]Work, 0), mtx:&sync.Mutex{}})
		wp.notif.Store(w.Key(), &sync.Mutex{})
		sem := xsync.NewWeighted(math.MaxInt64)
		wp.noWork.Store(w.Key(), sem)
		wp.isAlive.Store(w.Key(), false)

		err := sem.Acquire(context.TODO(), math.MaxInt64)
		must(err)
	}

	pool, _ := wp.pool.Load(w.Key())
	pool.(*workQueue).enqueue(w)

	atomic.AddUint64(wp.queueLen, 1)

	sem, _ := wp.noWork.Load(w.Key())
	sem.(*xsync.Weighted).Release(1)

	if isAlive, _ := wp.isAlive.Load(w.Key()); !isAlive.(bool){
		wp.isAlive.Store(w.Key(), true)
		go wp.manageKeyQueue(w.Key())
	}
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}