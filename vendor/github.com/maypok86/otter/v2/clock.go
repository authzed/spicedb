// Copyright (c) 2025 Alexey Mayshev and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otter

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2/internal/xmath"
)

// Clock is a time source that
//   - Returns a time value representing the number of nanoseconds elapsed since some
//     fixed but arbitrary point in time
//   - Returns a channel that delivers “ticks” of a clock at intervals.
type Clock interface {
	// NowNano returns the number of nanoseconds elapsed since this clock's fixed point of reference.
	//
	// By default, time.Now().UnixNano() is used.
	NowNano() int64
	// Tick returns a channel that delivers “ticks” of a clock at intervals.
	//
	// The cache uses this method only for proactive expiration and calls Tick(time.Second) in a separate goroutine.
	//
	// By default, [time.Tick] is used.
	Tick(duration time.Duration) <-chan time.Time
}

type timeSource interface {
	Clock
	Init()
	Sleep(duration time.Duration)
	ProcessTick()
}

func newTimeSource(clock Clock) timeSource {
	if clock == nil {
		return &realSource{}
	}
	if r, ok := clock.(*realSource); ok {
		return r
	}
	if f, ok := clock.(*fakeSource); ok {
		return f
	}
	return newCustomSource(clock)
}

type customSource struct {
	clock         Clock
	isInitialized atomic.Bool
}

func newCustomSource(clock Clock) *customSource {
	return &customSource{
		clock: clock,
	}
}

func (cs *customSource) Init() {
	if !cs.isInitialized.Load() {
		cs.isInitialized.Store(true)
	}
}

func (cs *customSource) NowNano() int64 {
	if !cs.isInitialized.Load() {
		return 0
	}
	return cs.clock.NowNano()
}

func (cs *customSource) Tick(duration time.Duration) <-chan time.Time {
	return cs.clock.Tick(duration)
}

func (cs *customSource) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

func (cs *customSource) ProcessTick() {}

type realSource struct {
	initMutex     sync.Mutex
	isInitialized atomic.Bool
	start         time.Time
	startNanos    atomic.Int64
}

func (c *realSource) Init() {
	if !c.isInitialized.Load() {
		c.initMutex.Lock()
		if !c.isInitialized.Load() {
			now := time.Now()
			c.start = now
			c.startNanos.Store(now.UnixNano())
			c.isInitialized.Store(true)
		}
		c.initMutex.Unlock()
	}
}

func (c *realSource) NowNano() int64 {
	if !c.isInitialized.Load() {
		return 0
	}
	return xmath.SaturatedAdd(c.startNanos.Load(), time.Since(c.start).Nanoseconds())
}

func (c *realSource) Tick(duration time.Duration) <-chan time.Time {
	return time.Tick(duration)
}

func (c *realSource) Sleep(duration time.Duration) {
	time.Sleep(duration)
}

func (c *realSource) ProcessTick() {}

type fakeSource struct {
	mutex          sync.Mutex
	now            time.Time
	initOnce       sync.Once
	sleeps         chan time.Duration
	tickWg         sync.WaitGroup
	sleepWg        sync.WaitGroup
	firstSleep     atomic.Bool
	withTick       atomic.Bool
	ticker         chan time.Time
	enableTickOnce sync.Once
	enableTick     chan time.Duration
}

func (f *fakeSource) Init() {
	f.initOnce.Do(func() {
		f.mutex.Lock()
		now := time.Now()
		f.now = now
		f.sleeps = make(chan time.Duration)
		f.firstSleep.Store(true)
		f.enableTick = make(chan time.Duration)
		f.ticker = make(chan time.Time, 1)
		f.mutex.Unlock()

		go func() {
			var (
				dur time.Duration
				d   time.Duration
			)
			enabled := false
			last := now
			for {
				select {
				case d = <-f.enableTick:
					enabled = true
					for d <= dur {
						if f.firstSleep.Load() {
							f.tickWg.Add(1)
							f.ticker <- last
							f.tickWg.Wait()
							f.firstSleep.Store(false)
						}
						last = last.Add(d)
						f.tickWg.Add(1)
						f.ticker <- last
						dur -= d
					}
				case s := <-f.sleeps:
					if enabled && f.firstSleep.Load() {
						f.tickWg.Add(1)
						f.ticker <- last
						f.tickWg.Wait()
						f.firstSleep.Store(false)
					}
					f.mutex.Lock()
					f.now = f.now.Add(s)
					f.mutex.Unlock()
					dur += s
					if enabled {
						for d <= dur {
							last = last.Add(d)
							f.tickWg.Add(1)
							f.ticker <- last
							dur -= d
						}
					}
					f.sleepWg.Done()
				}
			}
		}()
	})
}

func (f *fakeSource) NowNano() int64 {
	return f.getNow().UnixNano()
}

func (f *fakeSource) Tick(d time.Duration) <-chan time.Time {
	f.enableTickOnce.Do(func() {
		f.enableTick <- d
	})
	return f.ticker
}

func (f *fakeSource) Sleep(d time.Duration) {
	f.sleepWg.Add(1)
	f.sleeps <- d
	f.sleepWg.Wait()
}

func (f *fakeSource) getNow() time.Time {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.now
}

func (f *fakeSource) ProcessTick() {
	f.tickWg.Done()
}
