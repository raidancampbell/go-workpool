// package go_workpool implements a workpool synchronized on a work item's Key.
//in the course of the workpool's life, two times the number of unique keys (plus 1) can be created
//one goroutine per key max for parallel processing
//another goroutine per key max for work queue management
//one goroutine to manage the workpool
//TODO: a goroutine should die when there's no work and be recreated when more work is available
// a new bool map should be able to solve this
package go_workpool

import (
	"context"
	xsync "golang.org/x/sync/semaphore"
	"math"
	"sync"
	"sync/atomic"
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
	defer func() {
		wq.mtx.Lock()
		defer wq.mtx.Unlock()
		wq.queue = wq.queue[1:]
	}()
	return wq.queue[0]
}

func NewWorkpool() *Workpool {
	return &Workpool{
		queueLen: new(uint64),
		pool:     &sync.Map{},
		notif:    &sync.Map{},
		noWork:   &sync.Map{},
	}
}

// manages the work queue for a given key
//At max, there will be N active goroutines of manageKeyQueue, where N is the number of unique keys
func (wp *Workpool) manageKeyQueue(key string) {
	for {
		// lock this key's work. just make sure any earlier work on this key is already done
		notif, _ := wp.notif.Load(key)
		notif.(*sync.Mutex).Lock()

		// wait until there's actually any work.  If there's no work, then this will block until there is work
		nw, _ := wp.noWork.Load(key)
		err := nw.(*xsync.Weighted).Acquire(context.TODO(), 1)
		must(err)
		p, _ := wp.pool.Load(key)
		work := p.(*workQueue).deque()
		go func() {
			work.Do()
			atomic.AddUint64(wp.queueLen, ^uint64(0))
			notif, _ := wp.notif.Load(key)
			notif.(*sync.Mutex).Unlock()
		}()
	}
}

// Submit submits the given work to the workpool.  If other work is already in place with the same key, then this work
// will be queued.  Order is guaranteed as a FIFO queue.
func (wp *Workpool) Submit(w Work) {
	wp.submitMtx.Lock()//TODO: check if this is required
	defer wp.submitMtx.Unlock()

	// the notif map is recycled to indicate whether the key has ever been seen before
	if _, ok := wp.notif.Load(w.Key()); !ok {
		// if this is the first time we've seen this key, set everything up
		wp.pool.Store(w.Key(), &workQueue{queue: make([]Work, 0), mtx:&sync.Mutex{}})
		wp.notif.Store(w.Key(), &sync.Mutex{})
		sem := xsync.NewWeighted(math.MaxInt64)
		wp.noWork.Store(w.Key(), sem)

		err := sem.Acquire(context.TODO(), math.MaxInt64)
		must(err)
		go wp.manageKeyQueue(w.Key())
	}

	atomic.AddUint64(wp.queueLen, 1)

	pool, _ := wp.pool.Load(w.Key())
	pool.(*workQueue).enqueue(w)

	sem, _ := wp.noWork.Load(w.Key())
	sem.(*xsync.Weighted).Release(1)
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}