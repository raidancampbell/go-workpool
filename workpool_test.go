package go_workpool

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

type wrk struct {
	k string
	d func()
}

func (w wrk) Key() string {
	return w.k
}

func (w wrk) Do() {
	w.d()
}

func newRandomTestWork(wg *sync.WaitGroup) Work {
	return wrk{
		k: "newTestWork" + strconv.Itoa(rand.Int()),
		d: func() {
			wg.Done()
		},
	}
}

func TestSingleUnique(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	sut := NewWorkpool()
	ntw := newRandomTestWork(&wg)
	sut.Submit(ntw)
	wg.Wait()
	assert.Equal(t, uint64(0), *sut.queueLen)
}

func TestDoubleUnique(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	sut := NewWorkpool()
	sut.Submit(newRandomTestWork(&wg))
	sut.Submit(newRandomTestWork(&wg))
	wg.Wait()
	assert.Equal(t, uint64(0), *sut.queueLen)
}

func TestManyUnique(t *testing.T) {
	N := 10000
	wg := sync.WaitGroup{}
	wg.Add(N)
	sut := NewWorkpool()
	for i := 0; i<N; i++ {
		sut.Submit(newRandomTestWork(&wg))
	}
	wg.Wait()
	assert.Equal(t, uint64(0), *sut.queueLen)
}

func BenchmarkManyUnique(b *testing.B) {
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	sut := NewWorkpool()
	for i := 0; i<b.N; i++ {
		sut.Submit(newRandomTestWork(&wg))
	}
	wg.Wait()
}

func TestSingleDuplicate(t *testing.T) {

}

func TestDoubleDuplicate(t *testing.T) {

}

func TestManyDuplicate(t *testing.T) {

}

func TestHell(t *testing.T) {

}
