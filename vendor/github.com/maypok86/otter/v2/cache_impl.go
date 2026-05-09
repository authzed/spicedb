// Copyright (c) 2023 Alexey Mayshev and contributors. All rights reserved.
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
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2/internal/deque/queue"
	"github.com/maypok86/otter/v2/internal/expiration"
	"github.com/maypok86/otter/v2/internal/generated/node"
	"github.com/maypok86/otter/v2/internal/hashmap"
	"github.com/maypok86/otter/v2/internal/lossy"
	"github.com/maypok86/otter/v2/internal/xiter"
	"github.com/maypok86/otter/v2/internal/xmath"
	"github.com/maypok86/otter/v2/internal/xruntime"
	"github.com/maypok86/otter/v2/stats"
)

const (
	unreachableExpiresAt     = int64(xruntime.MaxDuration)
	unreachableRefreshableAt = int64(xruntime.MaxDuration)
	noTime                   = int64(0)

	minWriteBufferSize = 4
	writeBufferRetries = 100
)

const (
	// A drain is not taking place.
	idle uint32 = 0
	// A drain is required due to a pending write modification.
	required uint32 = 1
	// A drain is in progress and will transition to idle.
	processingToIdle uint32 = 2
	// A drain is in progress and will transition to required.
	processingToRequired uint32 = 3
)

var (
	maxWriteBufferSize   uint32
	maxStripedBufferSize int
)

func init() {
	parallelism := xruntime.Parallelism()
	roundedParallelism := int(xmath.RoundUpPowerOf2(parallelism))
	//nolint:gosec // there will never be an overflow
	maxWriteBufferSize = uint32(128 * roundedParallelism)
	maxStripedBufferSize = 4 * roundedParallelism
}

func zeroValue[V any]() V {
	var v V
	return v
}

// cache is a structure performs a best-effort bounding of a hash table using eviction algorithm
// to determine which entries to evict when the capacity is exceeded.
type cache[K comparable, V any] struct {
	drainStatus        atomic.Uint32
	_                  [xruntime.CacheLineSize - 4]byte
	nodeManager        *node.Manager[K, V]
	hashmap            *hashmap.Map[K, V, node.Node[K, V]]
	evictionPolicy     *policy[K, V]
	expirationPolicy   *expiration.Variable[K, V]
	stats              stats.Recorder
	statsSnapshoter    stats.Snapshoter
	logger             Logger
	clock              timeSource
	statsClock         *realSource
	readBuffer         *lossy.Striped[K, V]
	writeBuffer        *queue.MPSC[task[K, V]]
	executor           func(fn func())
	singleflight       *group[K, V]
	evictionMutex      sync.Mutex
	doneClose          chan struct{}
	weigher            func(key K, value V) uint32
	onDeletion         func(e DeletionEvent[K, V])
	onAtomicDeletion   func(e DeletionEvent[K, V])
	expiryCalculator   ExpiryCalculator[K, V]
	refreshCalculator  RefreshCalculator[K, V]
	taskPool           sync.Pool
	hasDefaultExecutor bool
	withTime           bool
	withExpiration     bool
	withRefresh        bool
	withEviction       bool
	isWeighted         bool
	withMaintenance    bool
	withStats          bool
}

// newCache returns a new cache instance based on the settings from Options.
func newCache[K comparable, V any](o *Options[K, V]) *cache[K, V] {
	withWeight := o.MaximumWeight > 0
	nodeManager := node.NewManager[K, V](node.Config{
		WithSize:       o.MaximumSize > 0,
		WithExpiration: o.ExpiryCalculator != nil,
		WithRefresh:    o.RefreshCalculator != nil,
		WithWeight:     withWeight,
	})

	maximum := o.getMaximum()
	withEviction := maximum > 0

	withStats := o.StatsRecorder != nil
	if withStats {
		_, ok := o.StatsRecorder.(*stats.NoopRecorder)
		withStats = !ok
	}
	statsRecorder := o.StatsRecorder
	if !withStats {
		statsRecorder = &stats.NoopRecorder{}
	}
	var statsSnapshoter stats.Snapshoter
	if snapshoter, ok := statsRecorder.(stats.Snapshoter); ok {
		statsSnapshoter = snapshoter
	} else {
		statsSnapshoter = &stats.NoopRecorder{}
	}

	c := &cache[K, V]{
		nodeManager:        nodeManager,
		hashmap:            hashmap.NewWithSize[K, V, node.Node[K, V]](nodeManager, o.getInitialCapacity()),
		stats:              statsRecorder,
		statsSnapshoter:    statsSnapshoter,
		logger:             o.getLogger(),
		singleflight:       &group[K, V]{},
		executor:           o.getExecutor(),
		hasDefaultExecutor: o.Executor == nil,
		weigher:            o.getWeigher(),
		onDeletion:         o.OnDeletion,
		onAtomicDeletion:   o.OnAtomicDeletion,
		clock:              newTimeSource(o.Clock),
		statsClock:         &realSource{},
		expiryCalculator:   o.ExpiryCalculator,
		refreshCalculator:  o.RefreshCalculator,
		isWeighted:         withWeight,
		withStats:          withStats,
	}

	if withStats {
		c.statsClock.Init()
	}

	c.withEviction = withEviction
	if c.withEviction {
		c.evictionPolicy = newPolicy[K, V](withWeight)
		if o.hasInitialCapacity() {
			//nolint:gosec // there's no overflow
			c.evictionPolicy.sketch.ensureCapacity(min(maximum, uint64(o.getInitialCapacity())))
		}
	}

	if o.ExpiryCalculator != nil {
		c.expirationPolicy = expiration.NewVariable(nodeManager)
	}

	c.withExpiration = o.ExpiryCalculator != nil
	c.withRefresh = o.RefreshCalculator != nil
	c.withTime = c.withExpiration || c.withRefresh
	c.withMaintenance = c.withEviction || c.withExpiration

	if c.withMaintenance {
		c.readBuffer = lossy.NewStriped(maxStripedBufferSize, nodeManager)
		c.writeBuffer = queue.NewMPSC[task[K, V]](minWriteBufferSize, maxWriteBufferSize)
	}
	if c.withTime {
		c.clock.Init()
	}
	if c.withExpiration {
		c.doneClose = make(chan struct{})
		go c.periodicCleanUp()
	}

	if c.withEviction {
		c.SetMaximum(maximum)
	}

	return c
}

func (c *cache[K, V]) newNode(key K, value V, old node.Node[K, V]) node.Node[K, V] {
	weight := c.weigher(key, value)
	expiresAt := unreachableExpiresAt
	if c.withExpiration && old != nil {
		expiresAt = old.ExpiresAt()
	}
	refreshableAt := unreachableRefreshableAt
	if c.withRefresh && old != nil {
		refreshableAt = old.RefreshableAt()
	}
	return c.nodeManager.Create(key, value, expiresAt, refreshableAt, weight)
}

func (c *cache[K, V]) nodeToEntry(n node.Node[K, V], nanos int64) Entry[K, V] {
	nowNano := noTime
	if c.withTime {
		nowNano = nanos
	}

	expiresAt := unreachableExpiresAt
	if c.withExpiration {
		expiresAt = n.ExpiresAt()
	}

	refreshableAt := unreachableRefreshableAt
	if c.withRefresh {
		refreshableAt = n.RefreshableAt()
	}

	return Entry[K, V]{
		Key:               n.Key(),
		Value:             n.Value(),
		Weight:            n.Weight(),
		ExpiresAtNano:     expiresAt,
		RefreshableAtNano: refreshableAt,
		SnapshotAtNano:    nowNano,
	}
}

// has checks if there is an item with the given key in the cache.
func (c *cache[K, V]) has(key K) bool {
	_, ok := c.GetIfPresent(key)
	return ok
}

// GetIfPresent returns the value associated with the key in this cache.
func (c *cache[K, V]) GetIfPresent(key K) (V, bool) {
	nowNano := c.clock.NowNano()
	n := c.getNode(key, nowNano)
	if n == nil {
		return zeroValue[V](), false
	}

	return n.Value(), true
}

// getNode returns the node associated with the key in this cache.
func (c *cache[K, V]) getNode(key K, nowNano int64) node.Node[K, V] {
	n := c.hashmap.Get(key)
	if n == nil {
		c.stats.RecordMisses(1)
		if c.drainStatus.Load() == required {
			c.scheduleDrainBuffers()
		}
		return nil
	}
	if n.HasExpired(nowNano) {
		c.stats.RecordMisses(1)
		c.scheduleDrainBuffers()
		return nil
	}

	c.afterRead(n, nowNano, true, true)

	return n
}

// getNodeQuietly returns the node associated with the key in this cache.
//
// Unlike getNode, this function does not produce any side effects
// such as updating statistics or the eviction policy.
func (c *cache[K, V]) getNodeQuietly(key K, nowNano int64) node.Node[K, V] {
	n := c.hashmap.Get(key)
	if n == nil || !n.IsAlive() || n.HasExpired(nowNano) {
		return nil
	}

	return n
}

func (c *cache[K, V]) afterRead(got node.Node[K, V], nowNano int64, recordHit, calcExpiresAt bool) {
	if recordHit {
		c.stats.RecordHits(1)
	}

	if calcExpiresAt {
		c.calcExpiresAtAfterRead(got, nowNano)
	}

	delayable := c.skipReadBuffer() || c.readBuffer.Add(got) != lossy.Full
	if c.shouldDrainBuffers(delayable) {
		c.scheduleDrainBuffers()
	}
}

// Set associates the value with the key in this cache.
//
// If the specified key is not already associated with a value, then it returns new value and true.
//
// If the specified key is already associated with a value, then it returns existing value and false.
func (c *cache[K, V]) Set(key K, value V) (V, bool) {
	return c.set(key, value, false)
}

// SetIfAbsent if the specified key is not already associated with a value associates it with the given value.
//
// If the specified key is not already associated with a value, then it returns new value and true.
//
// If the specified key is already associated with a value, then it returns existing value and false.
func (c *cache[K, V]) SetIfAbsent(key K, value V) (V, bool) {
	return c.set(key, value, true)
}

func (c *cache[K, V]) calcExpiresAtAfterRead(n node.Node[K, V], nowNano int64) {
	if !c.withExpiration {
		return
	}

	expiresAfter := c.expiryCalculator.ExpireAfterRead(c.nodeToEntry(n, nowNano))
	c.setExpiresAfterRead(n, nowNano, expiresAfter)
}

func (c *cache[K, V]) setExpiresAfterRead(n node.Node[K, V], nowNano int64, expiresAfter time.Duration) {
	if expiresAfter <= 0 {
		return
	}

	expiresAt := n.ExpiresAt()
	currentDuration := time.Duration(expiresAt - nowNano)
	diff := xmath.Abs(int64(expiresAfter - currentDuration))
	if diff > 0 {
		n.CASExpiresAt(expiresAt, nowNano+int64(expiresAfter))
	}
}

// GetEntry returns the cache entry associated with the key in this cache.
func (c *cache[K, V]) GetEntry(key K) (Entry[K, V], bool) {
	nowNano := c.clock.NowNano()
	n := c.getNode(key, nowNano)
	if n == nil {
		return Entry[K, V]{}, false
	}
	return c.nodeToEntry(n, nowNano), true
}

// GetEntryQuietly returns the cache entry associated with the key in this cache.
//
// Unlike GetEntry, this function does not produce any side effects
// such as updating statistics or the eviction policy.
func (c *cache[K, V]) GetEntryQuietly(key K) (Entry[K, V], bool) {
	nowNano := c.clock.NowNano()
	n := c.getNodeQuietly(key, nowNano)
	if n == nil {
		return Entry[K, V]{}, false
	}
	return c.nodeToEntry(n, nowNano), true
}

// SetExpiresAfter specifies that the entry should be automatically removed from the cache once the duration has
// elapsed. The expiration policy determines when the entry's age is reset.
func (c *cache[K, V]) SetExpiresAfter(key K, expiresAfter time.Duration) {
	if !c.withExpiration || expiresAfter <= 0 {
		return
	}

	nowNano := c.clock.NowNano()
	n := c.hashmap.Get(key)
	if n == nil {
		return
	}

	c.setExpiresAfterRead(n, nowNano, expiresAfter)
	c.afterRead(n, nowNano, false, false)
}

// SetRefreshableAfter specifies that each entry should be eligible for reloading once a fixed duration has elapsed.
// The refresh policy determines when the entry's age is reset.
func (c *cache[K, V]) SetRefreshableAfter(key K, refreshableAfter time.Duration) {
	if !c.withRefresh || refreshableAfter <= 0 {
		return
	}

	nowNano := c.clock.NowNano()
	n := c.hashmap.Get(key)
	if n == nil {
		return
	}

	entry := c.nodeToEntry(n, nowNano)
	currentDuration := entry.RefreshableAfter()
	if refreshableAfter > 0 && currentDuration != refreshableAfter {
		n.SetRefreshableAt(nowNano + int64(refreshableAfter))
	}
}

func (c *cache[K, V]) calcExpiresAtAfterWrite(n, old node.Node[K, V], nowNano int64) {
	if !c.withExpiration {
		return
	}

	entry := c.nodeToEntry(n, nowNano)
	currentDuration := entry.ExpiresAfter()
	var expiresAfter time.Duration
	if old == nil {
		expiresAfter = c.expiryCalculator.ExpireAfterCreate(entry)
	} else {
		expiresAfter = c.expiryCalculator.ExpireAfterUpdate(entry, old.Value())
	}

	if expiresAfter > 0 && currentDuration != expiresAfter {
		n.SetExpiresAt(nowNano + int64(expiresAfter))
	}
}

func (c *cache[K, V]) set(key K, value V, onlyIfAbsent bool) (V, bool) {
	var old node.Node[K, V]
	nowNano := c.clock.NowNano()
	n := c.hashmap.Compute(key, func(current node.Node[K, V]) node.Node[K, V] {
		old = current
		if onlyIfAbsent && current != nil && !current.HasExpired(nowNano) {
			// no op
			c.calcExpiresAtAfterRead(old, nowNano)
			return current
		}
		// set
		return c.atomicSet(key, value, old, nil, nowNano)
	})
	if onlyIfAbsent {
		if old == nil || old.HasExpired(nowNano) {
			c.afterWrite(n, old, nowNano)
			return value, true
		}
		c.afterRead(old, nowNano, false, false)
		return old.Value(), false
	}

	c.afterWrite(n, old, nowNano)
	if old != nil {
		return old.Value(), false
	}
	return value, true
}

func (c *cache[K, V]) atomicSet(key K, value V, old node.Node[K, V], cl *call[K, V], nowNano int64) node.Node[K, V] {
	if cl == nil {
		c.singleflight.delete(key)
	}
	n := c.newNode(key, value, old)
	c.calcExpiresAtAfterWrite(n, old, nowNano)
	c.calcRefreshableAt(n, old, cl, nowNano)
	c.makeRetired(old)
	if old != nil {
		cause := getCause(old, nowNano, CauseReplacement)
		c.notifyAtomicDeletion(old.Key(), old.Value(), cause)
	}
	return n
}

//nolint:unparam // it's ok
func (c *cache[K, V]) atomicDelete(key K, old node.Node[K, V], cl *call[K, V], nowNano int64) node.Node[K, V] {
	if cl == nil {
		c.singleflight.delete(key)
	}
	if old != nil {
		cause := getCause(old, nowNano, CauseInvalidation)
		c.makeRetired(old)
		c.notifyAtomicDeletion(old.Key(), old.Value(), cause)
	}
	return nil
}

// Compute either sets the computed new value for the key,
// invalidates the value for the key, or does nothing, based on
// the returned [ComputeOp]. When the op returned by remappingFunc
// is [WriteOp], the value is updated to the new value. If
// it is [InvalidateOp], the entry is removed from the cache
// altogether. And finally, if the op is [CancelOp] then the
// entry is left as-is. In other words, if it did not already
// exist, it is not created, and if it did exist, it is not
// updated. This is useful to synchronously execute some
// operation on the value without incurring the cost of
// updating the cache every time.
//
// The ok result indicates whether the entry is present in the cache after the compute operation.
// The actualValue result contains the value of the cache
// if a corresponding entry is present, or the zero value otherwise.
// You can think of these results as equivalent to regular key-value lookups in a map.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the remappingFunc executes. Consider
// this when the function includes long-running operations.
func (c *cache[K, V]) Compute(
	key K,
	remappingFunc func(oldValue V, found bool) (newValue V, op ComputeOp),
) (V, bool) {
	return c.doCompute(key, remappingFunc, c.clock.NowNano(), true)
}

// ComputeIfAbsent returns the existing value for the key if
// present. Otherwise, it tries to compute the value using the
// provided function. If mappingFunc returns true as the cancel value, the computation is cancelled and the zero value
// for type V is returned.
//
// The ok result indicates whether the entry is present in the cache after the compute operation.
// The actualValue result contains the value of the cache
// if a corresponding entry is present, or the zero value
// otherwise. You can think of these results as equivalent to regular key-value lookups in a map.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (c *cache[K, V]) ComputeIfAbsent(
	key K,
	mappingFunc func() (newValue V, cancel bool),
) (V, bool) {
	nowNano := c.clock.NowNano()
	if n := c.getNode(key, nowNano); n != nil {
		return n.Value(), true
	}

	return c.doCompute(key, func(oldValue V, found bool) (newValue V, op ComputeOp) {
		if found {
			return oldValue, CancelOp
		}
		newValue, cancel := mappingFunc()
		if cancel {
			return zeroValue[V](), CancelOp
		}
		return newValue, WriteOp
	}, nowNano, false)
}

// ComputeIfPresent returns the zero value for type V if the key is not found.
// Otherwise, it tries to compute the value using the provided function.
//
// ComputeIfPresent either sets the computed new value for the key,
// invalidates the value for the key, or does nothing, based on
// the returned [ComputeOp]. When the op returned by remappingFunc
// is [WriteOp], the value is updated to the new value. If
// it is [InvalidateOp], the entry is removed from the cache
// altogether. And finally, if the op is [CancelOp] then the
// entry is left as-is. In other words, if it did not already
// exist, it is not created, and if it did exist, it is not
// updated. This is useful to synchronously execute some
// operation on the value without incurring the cost of
// updating the cache every time.
//
// The ok result indicates whether the entry is present in the cache after the compute operation.
// The actualValue result contains the value of the cache
// if a corresponding entry is present, or the zero value
// otherwise. You can think of these results as equivalent to regular key-value lookups in a map.
//
// This call locks a hash table bucket while the compute function
// is executed. It means that modifications on other entries in
// the bucket will be blocked until the valueFn executes. Consider
// this when the function includes long-running operations.
func (c *cache[K, V]) ComputeIfPresent(
	key K,
	remappingFunc func(oldValue V) (newValue V, op ComputeOp),
) (V, bool) {
	nowNano := c.clock.NowNano()
	if n := c.getNode(key, nowNano); n == nil {
		return zeroValue[V](), false
	}

	return c.doCompute(key, func(oldValue V, found bool) (newValue V, op ComputeOp) {
		if found {
			return remappingFunc(oldValue)
		}
		return zeroValue[V](), CancelOp
	}, nowNano, false)
}

func (c *cache[K, V]) doCompute(
	key K,
	remappingFunc func(oldValue V, found bool) (newValue V, op ComputeOp),
	nowNano int64,
	recordStats bool,
) (V, bool) {
	var (
		old        node.Node[K, V]
		op         ComputeOp
		notValidOp bool
		panicErr   error
	)
	computedNode := c.hashmap.Compute(key, func(oldNode node.Node[K, V]) node.Node[K, V] {
		var (
			oldValue    V
			actualValue V
			found       bool
		)
		if oldNode != nil && !oldNode.HasExpired(nowNano) {
			oldValue = oldNode.Value()
			found = true
		}
		old = oldNode

		func() {
			defer func() {
				if r := recover(); r != nil {
					panicErr = newPanicError(r)
				}
			}()

			actualValue, op = remappingFunc(oldValue, found)
		}()
		if panicErr != nil {
			return oldNode
		}
		if op == CancelOp {
			if oldNode != nil && oldNode.HasExpired(nowNano) {
				return c.atomicDelete(key, oldNode, nil, nowNano)
			}
			return oldNode
		}
		if op == WriteOp {
			return c.atomicSet(key, actualValue, old, nil, nowNano)
		}
		if op == InvalidateOp {
			return c.atomicDelete(key, old, nil, nowNano)
		}
		notValidOp = true
		return oldNode
	})
	if panicErr != nil {
		panic(panicErr)
	}
	if notValidOp {
		panic(fmt.Sprintf("otter: invalid ComputeOp: %d", op))
	}
	if recordStats {
		if old != nil && !old.HasExpired(nowNano) {
			c.stats.RecordHits(1)
		} else {
			c.stats.RecordMisses(1)
		}
	}
	switch op {
	case CancelOp:
		if computedNode == nil {
			c.afterDelete(old, nowNano, false)
			return zeroValue[V](), false
		}
		return computedNode.Value(), true
	case WriteOp:
		c.afterWrite(computedNode, old, nowNano)
	case InvalidateOp:
		c.afterDelete(old, nowNano, false)
	}
	if computedNode == nil {
		return zeroValue[V](), false
	}
	return computedNode.Value(), true
}

func (c *cache[K, V]) afterWrite(n, old node.Node[K, V], nowNano int64) {
	if !c.withMaintenance {
		if old != nil {
			c.notifyDeletion(old.Key(), old.Value(), CauseReplacement)
		}
		return
	}

	if old == nil {
		// insert
		c.afterWriteTask(c.getTask(n, nil, addReason, causeUnknown))
		return
	}

	// update
	cause := getCause(old, nowNano, CauseReplacement)
	c.afterWriteTask(c.getTask(n, old, updateReason, cause))
}

type refreshableKey[K comparable, V any] struct {
	key K
	old node.Node[K, V]
}

func (c *cache[K, V]) refreshKey(
	ctx context.Context,
	rk refreshableKey[K, V],
	loader Loader[K, V],
	isManual bool,
) <-chan RefreshResult[K, V] {
	if !c.withRefresh {
		return nil
	}

	var ch chan RefreshResult[K, V]
	if isManual {
		ch = make(chan RefreshResult[K, V], 1)
	}

	c.executor(func() {
		var refresher func(ctx context.Context, key K) (V, error)
		if rk.old != nil {
			refresher = func(ctx context.Context, key K) (V, error) {
				return loader.Reload(ctx, key, rk.old.Value())
			}
		} else {
			refresher = loader.Load
		}

		cl, shouldLoad := c.singleflight.startCall(rk.key, true)
		if shouldLoad {
			//nolint:errcheck // there is no need to check error
			_ = c.wrapLoad(func() error {
				loadCtx := context.WithoutCancel(ctx)
				return c.singleflight.doCall(loadCtx, cl, refresher, c.afterDeleteCall)
			})
		}
		cl.wait()

		if cl.err != nil && !cl.isNotFound {
			c.logger.Error(ctx, "Returned an error during the refreshing", cl.err)
		}

		if isManual {
			ch <- RefreshResult[K, V]{
				Key:   cl.key,
				Value: cl.value,
				Err:   cl.err,
			}
		}
	})

	return ch
}

// Get returns the value associated with key in this cache, obtaining that value from loader if necessary.
// The method improves upon the conventional "if cached, return; otherwise create, cache and return" pattern.
//
// Get can return an ErrNotFound error if the Loader returns it.
// This means that the entry was not found in the data source.
//
// If another call to Get is currently loading the value for key,
// simply waits for that goroutine to finish and returns its loaded value. Note that
// multiple goroutines can concurrently load values for distinct keys.
//
// No observable state associated with this cache is modified until loading completes.
//
// WARNING: Loader.Load must not attempt to update any mappings of this cache directly.
//
// WARNING: For any given key, every loader used with it should compute the same value.
// Otherwise, a call that passes one loader may return the result of another call
// with a differently behaving loader. For example, a call that requests a short timeout
// for an RPC may wait for a similar call that requests a long timeout, or a call by an
// unprivileged user may return a resource accessible only to a privileged user making a similar call.
func (c *cache[K, V]) Get(ctx context.Context, key K, loader Loader[K, V]) (V, error) {
	c.singleflight.init()

	nowNano := c.clock.NowNano()
	n := c.getNode(key, nowNano)
	if n != nil {
		if !n.IsFresh(nowNano) {
			c.refreshKey(ctx, refreshableKey[K, V]{
				key: n.Key(),
				old: n,
			}, loader, false)
		}
		return n.Value(), nil
	}

	cl, shouldLoad := c.singleflight.startCall(key, false)
	if shouldLoad {
		//nolint:errcheck // there is no need to check error
		_ = c.wrapLoad(func() error {
			return c.singleflight.doCall(ctx, cl, loader.Load, c.afterDeleteCall)
		})
	}
	cl.wait()

	return cl.value, cl.err
}

func (c *cache[K, V]) calcRefreshableAt(n, old node.Node[K, V], cl *call[K, V], nowNano int64) {
	if !c.withRefresh {
		return
	}

	var refreshableAfter time.Duration
	entry := c.nodeToEntry(n, nowNano)
	currentDuration := entry.RefreshableAfter()
	//nolint:gocritic // it's ok
	if cl != nil && cl.isRefresh && old != nil {
		if cl.isNotFound {
			return
		}
		if cl.err != nil {
			refreshableAfter = c.refreshCalculator.RefreshAfterReloadFailure(entry, cl.err)
		} else {
			refreshableAfter = c.refreshCalculator.RefreshAfterReload(entry, old.Value())
		}
	} else if old != nil {
		refreshableAfter = c.refreshCalculator.RefreshAfterUpdate(entry, old.Value())
	} else {
		refreshableAfter = c.refreshCalculator.RefreshAfterCreate(entry)
	}

	if refreshableAfter > 0 && currentDuration != refreshableAfter {
		n.SetRefreshableAt(nowNano + int64(refreshableAfter))
	}
}

func (c *cache[K, V]) afterDeleteCall(cl *call[K, V]) {
	var (
		inserted bool
		deleted  bool
		old      node.Node[K, V]
	)
	nowNano := c.clock.NowNano()
	newNode := c.hashmap.Compute(cl.key, func(oldNode node.Node[K, V]) node.Node[K, V] {
		isCorrectCall := cl.isFake || c.singleflight.deleteCall(cl)
		old = oldNode
		if isCorrectCall && cl.isNotFound {
			deleted = oldNode != nil
			return c.atomicDelete(cl.key, oldNode, cl, nowNano)
		}
		if cl.err != nil {
			if cl.isRefresh && oldNode != nil {
				c.calcRefreshableAt(oldNode, oldNode, cl, nowNano)
			}
			return oldNode
		}
		if !isCorrectCall {
			return oldNode
		}
		inserted = true
		return c.atomicSet(cl.key, cl.value, old, cl, nowNano)
	})
	cl.cancel()
	if deleted {
		c.afterDelete(old, nowNano, false)
	}
	if inserted {
		c.afterWrite(newNode, old, nowNano)
	}
}

func (c *cache[K, V]) bulkRefreshKeys(
	ctx context.Context,
	rks []refreshableKey[K, V],
	bulkLoader BulkLoader[K, V],
	isManual bool,
) <-chan []RefreshResult[K, V] {
	if !c.withRefresh {
		return nil
	}
	var ch chan []RefreshResult[K, V]
	if isManual {
		ch = make(chan []RefreshResult[K, V], 1)
	}
	if len(rks) == 0 {
		if isManual {
			ch <- []RefreshResult[K, V]{}
		}
		return ch
	}

	c.executor(func() {
		var (
			toLoadCalls   map[K]*call[K, V]
			toReloadCalls map[K]*call[K, V]
			foundCalls    []*call[K, V]
			results       []RefreshResult[K, V]
		)
		if isManual {
			results = make([]RefreshResult[K, V], 0, len(rks))
		}
		i := 0
		for _, rk := range rks {
			cl, shouldLoad := c.singleflight.startCall(rk.key, true)
			if shouldLoad {
				if rk.old != nil {
					if toReloadCalls == nil {
						toReloadCalls = make(map[K]*call[K, V], len(rks)-i)
					}
					cl.value = rk.old.Value()
					toReloadCalls[rk.key] = cl
				} else {
					if toLoadCalls == nil {
						toLoadCalls = make(map[K]*call[K, V], len(rks)-i)
					}
					toLoadCalls[rk.key] = cl
				}
			} else {
				if foundCalls == nil {
					foundCalls = make([]*call[K, V], 0, len(rks)-i)
				}
				foundCalls = append(foundCalls, cl)
			}
			i++
		}

		loadCtx := context.WithoutCancel(ctx)
		if len(toLoadCalls) > 0 {
			loadErr := c.wrapLoad(func() error {
				return c.singleflight.doBulkCall(loadCtx, toLoadCalls, bulkLoader.BulkLoad, c.afterDeleteCall)
			})
			if loadErr != nil {
				c.logger.Error(ctx, "BulkLoad returned an error", loadErr)
			}

			if isManual {
				for _, cl := range toLoadCalls {
					results = append(results, RefreshResult[K, V]{
						Key:   cl.key,
						Value: cl.value,
						Err:   cl.err,
					})
				}
			}
		}
		if len(toReloadCalls) > 0 {
			reload := func(ctx context.Context, keys []K) (map[K]V, error) {
				oldValues := make([]V, 0, len(keys))
				for _, k := range keys {
					cl := toReloadCalls[k]
					oldValues = append(oldValues, cl.value)
					cl.value = zeroValue[V]()
				}
				return bulkLoader.BulkReload(ctx, keys, oldValues)
			}

			reloadErr := c.wrapLoad(func() error {
				return c.singleflight.doBulkCall(loadCtx, toReloadCalls, reload, c.afterDeleteCall)
			})
			if reloadErr != nil {
				c.logger.Error(ctx, "BulkReload returned an error", reloadErr)
			}

			if isManual {
				for _, cl := range toReloadCalls {
					results = append(results, RefreshResult[K, V]{
						Key:   cl.key,
						Value: cl.value,
						Err:   cl.err,
					})
				}
			}
		}
		for _, cl := range foundCalls {
			cl.wait()
			if isManual {
				results = append(results, RefreshResult[K, V]{
					Key:   cl.key,
					Value: cl.value,
					Err:   cl.err,
				})
			}
		}
		if isManual {
			ch <- results
		}
	})

	return ch
}

// BulkGet returns the value associated with key in this cache, obtaining that value from loader if necessary.
// The method improves upon the conventional "if cached, return; otherwise create, cache and return" pattern.
//
// If another call to Get (BulkGet) is currently loading the value for key,
// simply waits for that goroutine to finish and returns its loaded value. Note that
// multiple goroutines can concurrently load values for distinct keys.
//
// No observable state associated with this cache is modified until loading completes.
//
// WARNING: BulkLoader.BulkLoad must not attempt to update any mappings of this cache directly.
//
// WARNING: For any given key, every bulkLoader used with it should compute the same value.
// Otherwise, a call that passes one bulkLoader may return the result of another call
// with a differently behaving bulkLoader. For example, a call that requests a short timeout
// for an RPC may wait for a similar call that requests a long timeout, or a call by an
// unprivileged user may return a resource accessible only to a privileged user making a similar call.
func (c *cache[K, V]) BulkGet(ctx context.Context, keys []K, bulkLoader BulkLoader[K, V]) (map[K]V, error) {
	c.singleflight.init()

	nowNano := c.clock.NowNano()
	result := make(map[K]V, len(keys))
	var (
		misses    map[K]*call[K, V]
		toRefresh []refreshableKey[K, V]
	)
	for _, key := range keys {
		if _, found := result[key]; found {
			continue
		}
		if _, found := misses[key]; found {
			continue
		}

		n := c.getNode(key, nowNano)
		if n != nil {
			if !n.IsFresh(nowNano) {
				if toRefresh == nil {
					toRefresh = make([]refreshableKey[K, V], 0, len(keys)-len(result))
				}

				toRefresh = append(toRefresh, refreshableKey[K, V]{
					key: key,
					old: n,
				})
			}

			result[key] = n.Value()
			continue
		}

		if misses == nil {
			misses = make(map[K]*call[K, V], len(keys)-len(result))
		}
		misses[key] = nil
	}

	c.bulkRefreshKeys(ctx, toRefresh, bulkLoader, false)
	if len(misses) == 0 {
		return result, nil
	}

	var toLoadCalls map[K]*call[K, V]
	i := 0
	for key := range misses {
		cl, shouldLoad := c.singleflight.startCall(key, false)
		if shouldLoad {
			if toLoadCalls == nil {
				toLoadCalls = make(map[K]*call[K, V], len(misses)-i)
			}
			toLoadCalls[key] = cl
		}
		misses[key] = cl
		i++
	}

	var loadErr error
	if len(toLoadCalls) > 0 {
		loadErr = c.wrapLoad(func() error {
			return c.singleflight.doBulkCall(ctx, toLoadCalls, bulkLoader.BulkLoad, c.afterDeleteCall)
		})
	}
	if loadErr != nil {
		return result, loadErr
	}

	//nolint:prealloc // it's ok
	var errsFromCalls []error
	i = 0
	for key, cl := range misses {
		cl.wait()
		i++

		if cl.err == nil {
			result[key] = cl.Value()
			continue
		}
		if _, ok := toLoadCalls[key]; ok || cl.isNotFound {
			continue
		}
		if errsFromCalls == nil {
			errsFromCalls = make([]error, 0, len(misses)-i+1)
		}
		errsFromCalls = append(errsFromCalls, cl.err)
	}

	var err error
	if len(errsFromCalls) > 0 {
		err = errors.Join(errsFromCalls...)
	}

	return result, err
}

func (c *cache[K, V]) wrapLoad(fn func() error) error {
	startTime := c.statsClock.NowNano()

	err := fn()

	loadTime := time.Duration(c.statsClock.NowNano() - startTime)
	if err == nil || errors.Is(err, ErrNotFound) {
		c.stats.RecordLoadSuccess(loadTime)
	} else {
		c.stats.RecordLoadFailure(loadTime)
	}

	var pe *panicError
	if errors.As(err, &pe) {
		panic(pe)
	}

	return err
}

// Refresh loads a new value for the key, asynchronously. While the new value is loading the
// previous value (if any) will continue to be returned by any Get unless it is evicted.
// If the new value is loaded successfully, it will replace the previous value in the cache;
// If refreshing returned an error, the previous value will remain,
// and the error will be logged using Logger (if it's not ErrNotFound) and swallowed. If another goroutine is currently
// loading the value for key, then this method does not perform an additional load.
//
// cache will call Loader.Reload if the cache currently contains a value for the key,
// and Loader.Load otherwise.
//
// WARNING: Loader.Load and Loader.Reload must not attempt to update any mappings of this cache directly.
//
// WARNING: For any given key, every loader used with it should compute the same value.
// Otherwise, a call that passes one loader may return the result of another call
// with a differently behaving loader. For example, a call that requests a short timeout
// for an RPC may wait for a similar call that requests a long timeout, or a call by an
// unprivileged user may return a resource accessible only to a privileged user making a similar call.
func (c *cache[K, V]) Refresh(ctx context.Context, key K, loader Loader[K, V]) <-chan RefreshResult[K, V] {
	if !c.withRefresh {
		return nil
	}

	c.singleflight.init()

	nowNano := c.clock.NowNano()
	n := c.getNodeQuietly(key, nowNano)

	return c.refreshKey(ctx, refreshableKey[K, V]{
		key: key,
		old: n,
	}, loader, true)
}

// BulkRefresh loads a new value for each key, asynchronously. While the new value is loading the
// previous value (if any) will continue to be returned by any Get unless it is evicted.
// If the new value is loaded successfully, it will replace the previous value in the cache;
// If refreshing returned an error, the previous value will remain,
// and the error will be logged using Logger and swallowed. If another goroutine is currently
// loading the value for key, then this method does not perform an additional load.
//
// cache will call BulkLoader.BulkReload for existing keys, and BulkLoader.BulkLoad otherwise.
//
// WARNING: BulkLoader.BulkLoad and BulkLoader.BulkReload must not attempt to update any mappings of this cache directly.
//
// WARNING: For any given key, every bulkLoader used with it should compute the same value.
// Otherwise, a call that passes one bulkLoader may return the result of another call
// with a differently behaving loader. For example, a call that requests a short timeout
// for an RPC may wait for a similar call that requests a long timeout, or a call by an
// unprivileged user may return a resource accessible only to a privileged user making a similar call.
func (c *cache[K, V]) BulkRefresh(ctx context.Context, keys []K, bulkLoader BulkLoader[K, V]) <-chan []RefreshResult[K, V] {
	if !c.withRefresh {
		return nil
	}

	c.singleflight.init()

	uniq := make(map[K]struct{}, len(keys))
	for _, k := range keys {
		uniq[k] = struct{}{}
	}

	nowNano := c.clock.NowNano()
	toRefresh := make([]refreshableKey[K, V], 0, len(uniq))
	for key := range uniq {
		n := c.getNodeQuietly(key, nowNano)
		toRefresh = append(toRefresh, refreshableKey[K, V]{
			key: key,
			old: n,
		})
	}

	return c.bulkRefreshKeys(ctx, toRefresh, bulkLoader, true)
}

// Invalidate discards any cached value for the key.
//
// Returns previous value if any. The invalidated result reports whether the key was
// present.
func (c *cache[K, V]) Invalidate(key K) (value V, invalidated bool) {
	var d node.Node[K, V]
	nowNano := c.clock.NowNano()
	c.hashmap.Compute(key, func(n node.Node[K, V]) node.Node[K, V] {
		d = n
		return c.atomicDelete(key, d, nil, nowNano)
	})
	c.afterDelete(d, nowNano, false)
	if d != nil {
		return d.Value(), true
	}
	return zeroValue[V](), false
}

func (c *cache[K, V]) deleteNodeFromMap(n node.Node[K, V], nowNano int64, cause DeletionCause) node.Node[K, V] {
	var deleted node.Node[K, V]
	c.hashmap.Compute(n.Key(), func(current node.Node[K, V]) node.Node[K, V] {
		c.singleflight.delete(n.Key())
		if current == nil {
			return nil
		}
		if n.AsPointer() == current.AsPointer() {
			deleted = current
			cause := getCause(deleted, nowNano, cause)
			c.makeRetired(deleted)
			c.notifyAtomicDeletion(deleted.Key(), deleted.Value(), cause)
			return nil
		}
		return current
	})
	return deleted
}

func (c *cache[K, V]) deleteNode(n node.Node[K, V], nowNano int64) {
	c.afterDelete(c.deleteNodeFromMap(n, nowNano, CauseInvalidation), nowNano, true)
}

func (c *cache[K, V]) afterDelete(deleted node.Node[K, V], nowNano int64, alreadyLocked bool) {
	if deleted == nil {
		return
	}

	if !c.withMaintenance {
		c.notifyDeletion(deleted.Key(), deleted.Value(), CauseInvalidation)
		return
	}

	// delete
	cause := getCause(deleted, nowNano, CauseInvalidation)
	t := c.getTask(deleted, nil, deleteReason, cause)
	if alreadyLocked {
		c.runTask(t)
	} else {
		c.afterWriteTask(t)
	}
}

func (c *cache[K, V]) notifyDeletion(key K, value V, cause DeletionCause) {
	if c.onDeletion == nil {
		return
	}

	c.executor(func() {
		c.onDeletion(DeletionEvent[K, V]{
			Key:   key,
			Value: value,
			Cause: cause,
		})
	})
}

func (c *cache[K, V]) notifyAtomicDeletion(key K, value V, cause DeletionCause) {
	if c.onAtomicDeletion == nil {
		return
	}

	c.onAtomicDeletion(DeletionEvent[K, V]{
		Key:   key,
		Value: value,
		Cause: cause,
	})
}

func (c *cache[K, V]) periodicCleanUp() {
	tick := c.clock.Tick(time.Second)
	for {
		select {
		case <-c.doneClose:
			return
		case <-tick:
			c.CleanUp()
			c.clock.ProcessTick()
		}
	}
}

func (c *cache[K, V]) evictNode(n node.Node[K, V], nowNanos int64) {
	cause := CauseOverflow
	if n.HasExpired(nowNanos) {
		cause = CauseExpiration
	}

	deleted := c.deleteNodeFromMap(n, nowNanos, cause) != nil

	if c.withEviction {
		c.evictionPolicy.delete(n)
	}
	if c.withExpiration {
		c.expirationPolicy.Delete(n)
	}

	c.makeDead(n)

	if deleted {
		c.notifyDeletion(n.Key(), n.Value(), cause)
		c.stats.RecordEviction(n.Weight())
	}
}

func (c *cache[K, V]) nodes() iter.Seq[node.Node[K, V]] {
	return func(yield func(node.Node[K, V]) bool) {
		c.hashmap.Range(func(n node.Node[K, V]) bool {
			nowNano := c.clock.NowNano()
			if !n.IsAlive() || n.HasExpired(nowNano) {
				c.scheduleDrainBuffers()
				return true
			}

			return yield(n)
		})
	}
}

func (c *cache[K, V]) entries() iter.Seq[Entry[K, V]] {
	return func(yield func(Entry[K, V]) bool) {
		for n := range c.nodes() {
			if !yield(c.nodeToEntry(n, c.clock.NowNano())) {
				return
			}
		}
	}
}

// All returns an iterator over all entries in the cache.
//
// Iterator is at least weakly consistent: he is safe for concurrent use,
// but if the cache is modified (including by eviction) after the iterator is
// created, it is undefined which of the changes (if any) will be reflected in that iterator.
func (c *cache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for n := range c.nodes() {
			if !yield(n.Key(), n.Value()) {
				return
			}
		}
	}
}

// Keys returns an iterator over all keys in the cache.
// The iteration order is not specified and is not guaranteed to be the same from one call to the next.
//
// Iterator is at least weakly consistent: he is safe for concurrent use,
// but if the cache is modified (including by eviction) after the iterator is
// created, it is undefined which of the changes (if any) will be reflected in that iterator.
func (c *cache[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for n := range c.nodes() {
			if !yield(n.Key()) {
				return
			}
		}
	}
}

// Values returns an iterator over all values in the cache.
// The iteration order is not specified and is not guaranteed to be the same from one call to the next.
//
// Iterator is at least weakly consistent: he is safe for concurrent use,
// but if the cache is modified (including by eviction) after the iterator is
// created, it is undefined which of the changes (if any) will be reflected in that iterator.
func (c *cache[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		for n := range c.nodes() {
			if !yield(n.Value()) {
				return
			}
		}
	}
}

// InvalidateAll discards all entries in the cache. The behavior of this operation is undefined for an entry
// that is being loaded (or reloaded) and is otherwise not present.
func (c *cache[K, V]) InvalidateAll() {
	c.evictionMutex.Lock()

	if c.withMaintenance {
		c.readBuffer.DrainTo(func(n node.Node[K, V]) {})
		for {
			t := c.writeBuffer.TryPop()
			if t == nil {
				break
			}
			c.runTask(t)
		}
	}
	// Discard all entries, falling back to one-by-one to avoid excessive lock hold times
	nodes := make([]node.Node[K, V], 0, c.EstimatedSize())
	threshold := uint64(maxWriteBufferSize / 2)
	c.hashmap.Range(func(n node.Node[K, V]) bool {
		nodes = append(nodes, n)
		return true
	})
	nowNano := c.clock.NowNano()
	for len(nodes) > 0 && c.writeBuffer.Size() < threshold {
		n := nodes[len(nodes)-1]
		nodes = nodes[:len(nodes)-1]
		c.deleteNode(n, nowNano)
	}

	c.evictionMutex.Unlock()

	for _, n := range nodes {
		c.Invalidate(n.Key())
	}
}

// CleanUp performs any pending maintenance operations needed by the cache. Exactly which activities are
// performed -- if any -- is implementation-dependent.
func (c *cache[K, V]) CleanUp() {
	c.performCleanUp(nil)
}

func (c *cache[K, V]) shouldDrainBuffers(delayable bool) bool {
	drainStatus := c.drainStatus.Load()
	switch drainStatus {
	case idle:
		return !delayable
	case required:
		return true
	case processingToIdle, processingToRequired:
		return false
	default:
		panic(fmt.Sprintf("Invalid drain status: %d", drainStatus))
	}
}

func (c *cache[K, V]) skipReadBuffer() bool {
	return !c.withMaintenance || // without read buffer
		(!c.withExpiration && c.withEviction && c.evictionPolicy.sketch.isNotInitialized())
}

func (c *cache[K, V]) afterWriteTask(t *task[K, V]) {
	for i := 0; i < writeBufferRetries; i++ {
		if c.writeBuffer.TryPush(t) {
			c.scheduleAfterWrite()
			return
		}
		c.scheduleDrainBuffers()
		runtime.Gosched()
	}

	// In scenarios where the writing goroutines cannot make progress then they attempt to provide
	// assistance by performing the eviction work directly. This can resolve cases where the
	// maintenance task is scheduled but not running.
	c.performCleanUp(t)
}

func (c *cache[K, V]) scheduleAfterWrite() {
	for {
		drainStatus := c.drainStatus.Load()
		switch drainStatus {
		case idle:
			c.drainStatus.CompareAndSwap(idle, required)
			c.scheduleDrainBuffers()
			return
		case required:
			c.scheduleDrainBuffers()
			return
		case processingToIdle:
			if c.drainStatus.CompareAndSwap(processingToIdle, processingToRequired) {
				return
			}
		case processingToRequired:
			return
		default:
			panic(fmt.Sprintf("Invalid drain status: %d", drainStatus))
		}
	}
}

func (c *cache[K, V]) scheduleDrainBuffers() {
	if c.drainStatus.Load() >= processingToIdle {
		return
	}

	if c.evictionMutex.TryLock() {
		drainStatus := c.drainStatus.Load()
		if drainStatus >= processingToIdle {
			c.evictionMutex.Unlock()
			return
		}

		c.drainStatus.Store(processingToIdle)

		var token atomic.Uint32
		c.executor(func() {
			c.drainBuffers(&token)
		})

		if token.CompareAndSwap(0, 1) {
			c.evictionMutex.Unlock()
		}
	}
}

func (c *cache[K, V]) drainBuffers(token *atomic.Uint32) {
	if c.evictionMutex.TryLock() {
		c.maintenance(nil)
		c.evictionMutex.Unlock()
		c.rescheduleCleanUpIfIncomplete()
	} else {
		// already locked
		if token.CompareAndSwap(0, 1) {
			// executor is sync
			c.maintenance(nil)
			c.evictionMutex.Unlock()
			c.rescheduleCleanUpIfIncomplete()
		} else {
			// executor is async
			c.performCleanUp(nil)
		}
	}
}

func (c *cache[K, V]) performCleanUp(t *task[K, V]) {
	c.evictionMutex.Lock()
	c.maintenance(t)
	c.evictionMutex.Unlock()
	c.rescheduleCleanUpIfIncomplete()
}

func (c *cache[K, V]) rescheduleCleanUpIfIncomplete() {
	if c.drainStatus.Load() != required {
		return
	}

	// An immediate scheduling cannot be performed on a custom executor because it may use a
	// caller-runs policy. This could cause the caller's penalty to exceed the amortized threshold,
	// e.g. repeated concurrent writes could result in a retry loop.
	if c.hasDefaultExecutor {
		c.scheduleDrainBuffers()
		return
	}
}

func (c *cache[K, V]) maintenance(t *task[K, V]) {
	c.drainStatus.Store(processingToIdle)

	c.drainReadBuffer()
	c.drainWriteBuffer()
	c.runTask(t)
	c.expireNodes()
	c.evictNodes()
	c.climb()

	if c.drainStatus.Load() != processingToIdle || !c.drainStatus.CompareAndSwap(processingToIdle, idle) {
		c.drainStatus.Store(required)
	}
}

func (c *cache[K, V]) drainReadBuffer() {
	if c.skipReadBuffer() {
		return
	}

	c.readBuffer.DrainTo(c.onAccess)
}

func (c *cache[K, V]) drainWriteBuffer() {
	if !c.withMaintenance {
		return
	}

	for i := uint32(0); i <= maxWriteBufferSize; i++ {
		t := c.writeBuffer.TryPop()
		if t == nil {
			return
		}
		c.runTask(t)
	}
	c.drainStatus.Store(processingToRequired)
}

func (c *cache[K, V]) runTask(t *task[K, V]) {
	if t == nil {
		return
	}

	n := t.node()
	switch t.writeReason {
	case addReason:
		if c.withExpiration && n.IsAlive() {
			c.expirationPolicy.Add(n)
		}
		if c.withEviction {
			c.evictionPolicy.add(n, c.evictNode)
		}
	case updateReason:
		old := t.oldNode()
		if c.withExpiration {
			c.expirationPolicy.Delete(old)
			if n.IsAlive() {
				c.expirationPolicy.Add(n)
			}
		}
		if c.withEviction {
			c.evictionPolicy.update(n, old, c.evictNode)
		}
		c.notifyDeletion(old.Key(), old.Value(), t.deletionCause)
	case deleteReason:
		if c.withExpiration {
			c.expirationPolicy.Delete(n)
		}
		if c.withEviction {
			c.evictionPolicy.delete(n)
		}
		c.notifyDeletion(n.Key(), n.Value(), t.deletionCause)
	default:
		panic(fmt.Sprintf("Invalid task type: %d", t.writeReason))
	}

	c.putTask(t)
}

func (c *cache[K, V]) onAccess(n node.Node[K, V]) {
	if c.withEviction {
		c.evictionPolicy.access(n)
	}
	if c.withExpiration && !node.Equals(n.NextExp(), nil) {
		c.expirationPolicy.Delete(n)
		if n.IsAlive() {
			c.expirationPolicy.Add(n)
		}
	}
}

func (c *cache[K, V]) expireNodes() {
	if c.withExpiration {
		c.expirationPolicy.DeleteExpired(c.clock.NowNano(), c.evictNode)
	}
}

func (c *cache[K, V]) evictNodes() {
	if !c.withEviction {
		return
	}
	c.evictionPolicy.evictNodes(c.evictNode)
}

func (c *cache[K, V]) climb() {
	if !c.withEviction {
		return
	}
	c.evictionPolicy.climb()
}

func (c *cache[K, V]) getTask(n, old node.Node[K, V], writeReason reason, cause DeletionCause) *task[K, V] {
	t, ok := c.taskPool.Get().(*task[K, V])
	if !ok {
		return &task[K, V]{
			n:             n,
			old:           old,
			writeReason:   writeReason,
			deletionCause: cause,
		}
	}
	t.n = n
	t.old = old
	t.writeReason = writeReason
	t.deletionCause = cause

	return t
}

func (c *cache[K, V]) putTask(t *task[K, V]) {
	t.n = nil
	t.old = nil
	t.writeReason = unknownReason
	t.deletionCause = causeUnknown
	c.taskPool.Put(t)
}

// SetMaximum specifies the maximum total size of this cache. This value may be interpreted as the weighted
// or unweighted threshold size based on how this cache was constructed. If the cache currently
// exceeds the new maximum size this operation eagerly evict entries until the cache shrinks to
// the appropriate size.
func (c *cache[K, V]) SetMaximum(maximum uint64) {
	if !c.withEviction {
		return
	}
	c.evictionMutex.Lock()
	c.evictionPolicy.setMaximumSize(maximum)
	c.maintenance(nil)
	c.evictionMutex.Unlock()
	c.rescheduleCleanUpIfIncomplete()
}

// GetMaximum returns the maximum total weighted or unweighted size of this cache, depending on how the
// cache was constructed. If this cache does not use a (weighted) size bound, then the method will return math.MaxUint64.
func (c *cache[K, V]) GetMaximum() uint64 {
	if !c.withEviction {
		return uint64(math.MaxUint64)
	}

	c.evictionMutex.Lock()
	if c.drainStatus.Load() == required {
		c.maintenance(nil)
	}
	result := c.evictionPolicy.maximum
	c.evictionMutex.Unlock()
	c.rescheduleCleanUpIfIncomplete()
	return result
}

// close discards all entries in the cache and stop all goroutines.
//
// NOTE: this operation must be performed when no requests are made to the cache otherwise the behavior is undefined.
func (c *cache[K, V]) close() {
	if c.withExpiration {
		c.doneClose <- struct{}{}
	}
}

// EstimatedSize returns the approximate number of entries in this cache. The value returned is an estimate; the
// actual count may differ if there are concurrent insertions or deletions, or if some entries are
// pending deletion due to expiration. In the case of stale entries
// this inaccuracy can be mitigated by performing a CleanUp first.
func (c *cache[K, V]) EstimatedSize() int {
	return c.hashmap.Size()
}

// IsWeighted returns whether the cache is bounded by a maximum size or maximum weight.
func (c *cache[K, V]) IsWeighted() bool {
	return c.isWeighted
}

// IsRecordingStats returns whether the cache statistics are being accumulated.
func (c *cache[K, V]) IsRecordingStats() bool {
	return c.withStats
}

// Stats returns a current snapshot of this cache's cumulative statistics.
// All statistics are initialized to zero and are monotonically increasing over the lifetime of the cache.
// Due to the performance penalty of maintaining statistics,
// some implementations may not record the usage history immediately or at all.
//
// NOTE: If your [stats.Recorder] implementation doesn't also implement [stats.Snapshoter],
// this method will always return a zero-value snapshot.
func (c *cache[K, V]) Stats() stats.Stats {
	return c.statsSnapshoter.Snapshot()
}

// WeightedSize returns the approximate accumulated weight of entries in this cache. If this cache does not
// use a weighted size bound, then the method will return 0.
func (c *cache[K, V]) WeightedSize() uint64 {
	if !c.isWeighted {
		return 0
	}

	c.evictionMutex.Lock()
	if c.drainStatus.Load() == required {
		c.maintenance(nil)
	}
	result := c.evictionPolicy.weightedSize
	c.evictionMutex.Unlock()
	c.rescheduleCleanUpIfIncomplete()
	return result
}

// Hottest returns an iterator for ordered traversal of the cache entries. The order of
// iteration is from the entries most likely to be retained (hottest) to the entries least
// likely to be retained (coldest). This order is determined by the eviction policy's best guess
// at the start of the iteration.
//
// WARNING: Beware that this iteration is performed within the eviction policy's exclusive lock, so the
// iteration should be short and simple. While the iteration is in progress further eviction
// maintenance will be halted.
func (c *cache[K, V]) Hottest() iter.Seq[Entry[K, V]] {
	return c.evictionOrder(true)
}

// Coldest returns an iterator for ordered traversal of the cache entries. The order of
// iteration is from the entries least likely to be retained (coldest) to the entries most
// likely to be retained (hottest). This order is determined by the eviction policy's best guess
// at the start of the iteration.
//
// WARNING: Beware that this iteration is performed within the eviction policy's exclusive lock, so the
// iteration should be short and simple. While the iteration is in progress further eviction
// maintenance will be halted.
func (c *cache[K, V]) Coldest() iter.Seq[Entry[K, V]] {
	return c.evictionOrder(false)
}

func (c *cache[K, V]) evictionOrder(hottest bool) iter.Seq[Entry[K, V]] {
	if !c.withEviction {
		return c.entries()
	}

	return func(yield func(Entry[K, V]) bool) {
		comparator := func(a node.Node[K, V], b node.Node[K, V]) int {
			return cmp.Compare(
				c.evictionPolicy.sketch.frequency(a.Key()),
				c.evictionPolicy.sketch.frequency(b.Key()),
			)
		}

		var seq iter.Seq[node.Node[K, V]]
		if hottest {
			secondary := xiter.MergeFunc(
				c.evictionPolicy.probation.Backward(),
				c.evictionPolicy.window.Backward(),
				comparator,
			)
			seq = xiter.Concat(
				c.evictionPolicy.protected.Backward(),
				secondary,
			)
		} else {
			primary := xiter.MergeFunc(
				c.evictionPolicy.window.All(),
				c.evictionPolicy.probation.All(),
				func(a node.Node[K, V], b node.Node[K, V]) int {
					return -comparator(a, b)
				},
			)

			seq = xiter.Concat(
				primary,
				c.evictionPolicy.protected.All(),
			)
		}

		c.evictionMutex.Lock()
		defer c.evictionMutex.Unlock()
		c.maintenance(nil)

		for n := range seq {
			nowNano := c.clock.NowNano()
			if !n.IsAlive() || n.HasExpired(nowNano) {
				continue
			}
			if !yield(c.nodeToEntry(n, nowNano)) {
				return
			}
		}
	}
}

func (c *cache[K, V]) makeRetired(n node.Node[K, V]) {
	if n != nil && c.withMaintenance && n.IsAlive() {
		n.Retire()
	}
}

func (c *cache[K, V]) makeDead(n node.Node[K, V]) {
	if !c.withMaintenance {
		return
	}

	if c.withEviction {
		c.evictionPolicy.makeDead(n)
	} else if !n.IsDead() {
		n.Die()
	}
}

func getCause[K comparable, V any](n node.Node[K, V], nowNano int64, cause DeletionCause) DeletionCause {
	if n.HasExpired(nowNano) {
		return CauseExpiration
	}
	return cause
}
