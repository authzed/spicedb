// Copyright (c) 2023 Alexey Mayshev and contributors. All rights reserved.
// Copyright (c) 2021 Andrey Pechkurov. All rights reserved.
//
// Copyright notice. This code is a fork of xsync.Adder from this file with some changes:
// https://github.com/puzpuzpuz/xsync/blob/main/counter.go
//
// Use of this source code is governed by a MIT license that can be found
// at https://github.com/puzpuzpuz/xsync/blob/main/LICENSE

package xsync

import (
	"sync"
	"sync/atomic"

	"github.com/maypok86/otter/v2/internal/xmath"
	"github.com/maypok86/otter/v2/internal/xruntime"
)

// pool for P tokens.
var tokenPool sync.Pool

// a P token is used to point at the current OS thread (P)
// on which the goroutine is run; exact identity of the thread,
// as well as P migration tolerance, is not important since
// it's used to as a best effort mechanism for assigning
// concurrent operations (goroutines) to different stripes of
// the Adder.
type token struct {
	idx     uint32
	padding [xruntime.CacheLineSize - 4]byte
}

// A Adder is a striped int64 Adder.
//
// Should be preferred over a single atomically updated uint64
// Adder in high contention scenarios.
//
// A Adder must not be copied after first use.
type Adder struct {
	stripes []astripe
	mask    uint32
}

const cacheLineSize = 64

type astripe struct {
	adder   atomic.Uint64
	padding [cacheLineSize - 8]byte
}

// NewAdder creates a new Adder instance.
func NewAdder() *Adder {
	nstripes := xmath.RoundUpPowerOf2(xruntime.Parallelism())
	return &Adder{
		stripes: make([]astripe, nstripes),
		mask:    nstripes - 1,
	}
}

// Add adds the delta to the Adder.
func (a *Adder) Add(delta uint64) {
	t, ok := tokenPool.Get().(*token)
	if !ok {
		t = &token{
			idx: xruntime.Fastrand(),
		}
	}
	for {
		stripe := &a.stripes[t.idx&a.mask]
		cnt := stripe.adder.Load()
		if stripe.adder.CompareAndSwap(cnt, cnt+delta) {
			break
		}
		// Give a try with another randomly selected stripe.
		t.idx = xruntime.Fastrand()
	}
	tokenPool.Put(t)
}

// Value returns the current Adder value.
// The returned value may not include all of the latest operations in
// presence of concurrent modifications of the Adder.
func (a *Adder) Value() uint64 {
	value := uint64(0)
	for i := 0; i < len(a.stripes); i++ {
		stripe := &a.stripes[i]
		value += stripe.adder.Load()
	}
	return value
}
