// Copyright (c) 2024 Alexey Mayshev and contributors. All rights reserved.
// Copyright 2009 The Go Authors. All rights reserved.
//
// Copyright notice. Initial version of the following code was based on
// the following file from the Go Programming Language core repo:
// https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:src/container/list/list_test.go
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// That can be found at https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:LICENSE

package otter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/maypok86/otter/v2/internal/hashmap"
)

type call[K comparable, V any] struct {
	key        K
	value      V
	err        error
	wg         sync.WaitGroup
	isRefresh  bool
	isNotFound bool
	isFake     bool
}

func newCall[K comparable, V any](key K, isRefresh bool) *call[K, V] {
	c := &call[K, V]{
		key:       key,
		isRefresh: isRefresh,
	}
	c.wg.Add(1)
	return c
}

func (c *call[K, V]) Key() K {
	return c.key
}

func (c *call[K, V]) Value() V {
	return c.value
}

func (c *call[K, V]) AsPointer() unsafe.Pointer {
	//nolint:gosec // it's ok
	return unsafe.Pointer(c)
}

func (c *call[K, V]) cancel() {
	if c.isFake {
		return
	}
	c.wg.Done()
}

func (c *call[K, V]) wait() {
	c.wg.Wait()
}

type mapCallManager[K comparable, V any] struct{}

func (m *mapCallManager[K, V]) FromPointer(ptr unsafe.Pointer) *call[K, V] {
	return (*call[K, V])(ptr)
}

func (m *mapCallManager[K, V]) IsNil(c *call[K, V]) bool {
	return c == nil
}

type group[K comparable, V any] struct {
	calls         *hashmap.Map[K, V, *call[K, V]]
	initMutex     sync.Mutex
	isInitialized atomic.Bool
}

func (g *group[K, V]) init() {
	if !g.isInitialized.Load() {
		g.initMutex.Lock()
		if !g.isInitialized.Load() {
			g.calls = hashmap.New[K, V, *call[K, V]](&mapCallManager[K, V]{})
			g.isInitialized.Store(true)
		}
		g.initMutex.Unlock()
	}
}

func (g *group[K, V]) getCall(key K) *call[K, V] {
	return g.calls.Get(key)
}

func (g *group[K, V]) startCall(key K, isRefresh bool) (c *call[K, V], shouldLoad bool) {
	// fast path
	if c := g.getCall(key); c != nil {
		return c, shouldLoad
	}

	return g.calls.Compute(key, func(prevCall *call[K, V]) *call[K, V] {
		// double check
		if prevCall != nil {
			return prevCall
		}
		shouldLoad = true
		return newCall[K, V](key, isRefresh)
	}), shouldLoad
}

func (g *group[K, V]) doCall(
	ctx context.Context,
	c *call[K, V],
	load func(ctx context.Context, key K) (V, error),
	afterFinish func(c *call[K, V]),
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}

		c.err = err
		c.isNotFound = errors.Is(err, ErrNotFound)
		afterFinish(c)
	}()

	c.value, err = load(ctx, c.key)
	return err
}

func (g *group[K, V]) doBulkCall(
	ctx context.Context,
	callsInBulk map[K]*call[K, V],
	bulkLoad func(ctx context.Context, keys []K) (map[K]V, error),
	afterFinish func(c *call[K, V]),
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = newPanicError(r)
		}

		if err != nil {
			for _, cl := range callsInBulk {
				cl.err = err
				cl.isNotFound = false
			}
		}

		for _, cl := range callsInBulk {
			afterFinish(cl)
		}
	}()

	keys := make([]K, 0, len(callsInBulk))
	for k := range callsInBulk {
		keys = append(keys, k)
	}

	res, err := bulkLoad(ctx, keys)

	var (
		isRefresh bool
		found     bool
	)
	for k, cl := range callsInBulk {
		if !found {
			isRefresh = cl.isRefresh
			found = true
		}
		v, ok := res[k]
		if ok {
			cl.value = v
		} else {
			cl.isNotFound = true
		}
	}

	for k, v := range res {
		if _, ok := callsInBulk[k]; ok {
			continue
		}
		callsInBulk[k] = &call[K, V]{
			key:       k,
			value:     v,
			isFake:    true,
			isRefresh: isRefresh,
		}
	}

	return err
}

func (g *group[K, V]) deleteCall(c *call[K, V]) (deleted bool) {
	// fast path
	if got := g.getCall(c.key); got != c {
		return false
	}

	cl := g.calls.Compute(c.key, func(prevCall *call[K, V]) *call[K, V] {
		// double check
		if prevCall == c {
			// delete
			return nil
		}
		return prevCall
	})
	return cl == nil
}

func (g *group[K, V]) delete(key K) {
	if !g.isInitialized.Load() {
		return
	}

	g.calls.Compute(key, func(prevCall *call[K, V]) *call[K, V] {
		return nil
	})
}
