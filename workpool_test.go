package go_workpool

import (
	"github.com/stretchr/testify/assert"
	"math"
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

type system struct {
	values *sync.Map
}
func newSystem() *system {
	return &system{values:&sync.Map{}}
}

// getValue is a simple helper to show the current value for the given key
func (s *system) getValue(k string) int {
	load, ok := s.values.Load(k)
	if !ok {
		panic("expected input value should exist!")
	}
	return load.(int)
}

// newWorkForKey creates a slice of work for the given key, returning both the work and the expected final value
func (s *system) newWorkForKey(wg *sync.WaitGroup, k string) ([]Work, int){
	s.values.Store(k, 0)
	var w []Work
	v := 0
	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, s.getValue(k) + 1)
		},
	})
	v = 1

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, s.getValue(k) + 1)
		},
	})
	v = 2

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, int(math.Pow(float64(s.getValue(k)), float64(2))))
		},
	})
	v = 4

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, s.getValue(k) * 2)
		},
	})
	v = 8

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, int(math.Pow(float64(s.getValue(k)), float64(2))))
		},
	})
	v = 64

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, s.getValue(k) - 2)
		},
	})
	v = 62

	w = append(w, wrk{
		k: k,
		d: func() {
			wg.Done()
			s.values.Store(k, s.getValue(k) / 2)
		},
	})
	v = 31

	return w, v
}

func TestSingleUnique(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	sut := New()
	ntw := newRandomTestWork(&wg)
	sut.Submit(ntw)
	wg.Wait()
	//assert.Equal(t, uint64(0), *sut.queueLen)
}

func TestDoubleUnique(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	sut := New()
	sut.Submit(newRandomTestWork(&wg))
	sut.Submit(newRandomTestWork(&wg))
	wg.Wait()
	//assert.Equal(t, uint64(0), *sut.queueLen)
}

func TestManyUnique(t *testing.T) {
	N := 10000
	wg := sync.WaitGroup{}
	wg.Add(N)
	sut := New()
	for i := 0; i<N; i++ {
		sut.Submit(newRandomTestWork(&wg))
	}
	wg.Wait()
	//assert.Equal(t, uint64(0), *sut.queueLen)
}

func BenchmarkManyUnique(b *testing.B) {
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	sut := New()
	for i := 0; i<b.N; i++ {
		sut.Submit(newRandomTestWork(&wg))
	}
	wg.Wait()
}

func TestDuplicate(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(7 * 2)
	sut := New()
	s := newSystem()
	w, expected1 := s.newWorkForKey(&wg, "key1")
	for _, unit := range w {
		sut.Submit(unit)
	}

	w, expected2 := s.newWorkForKey(&wg, "key2")
	for _, unit := range w {
		sut.Submit(unit)
	}
	wg.Wait()
	assert.Equal(t, expected1, s.getValue("key1"))
	assert.Equal(t, expected2, s.getValue("key2"))
	//assert.Equal(t, uint64(0), *sut.queueLen)
}

func TestManyDuplicate(t *testing.T) {
	N := 10000
	wg := sync.WaitGroup{}
	wg.Add(7 * N)
	sut := New()
	s := newSystem()
	var expecteds []int

	for i := 0; i<N; i++ {
		w, exp := s.newWorkForKey(&wg, strconv.Itoa(i))
		expecteds = append(expecteds, exp)
		for _, unit := range w {
			sut.Submit(unit)
		}
	}
	wg.Wait()
	for i := 0; i<N; i++ {
		actual := s.getValue(strconv.Itoa(i))
		assert.Equal(t, expecteds[i], actual)
	}

}

func BenchmarkManyDuplicate(b *testing.B) {
	wg := sync.WaitGroup{}
	wg.Add(7 * b.N)
	sut := New()
	s := newSystem()

	for i := 0; i<b.N; i++ {
		w, _ := s.newWorkForKey(&wg, strconv.Itoa(i))
		for _, unit := range w {
			sut.Submit(unit)
		}
	}
	wg.Wait()
}
