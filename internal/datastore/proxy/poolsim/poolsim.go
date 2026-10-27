// Package poolsim provides a datastore proxy that models a fixed-size database
// connection pool in front of any datastore, so that code which holds one pooled
// connection while requesting another can be detected deterministically in tests,
// regardless of the backing datastore.
//
// The SQL datastores check a connection out of their pool for the full lifetime of
// a relationship iterator: rows are streamed off the open connection while the
// consumer's loop body runs. Any datastore access made from inside that loop body
// therefore needs a *second* connection from the same pool. If every connection in
// the pool is held by such a caller, none can make progress and the pool deadlocks
// (see https://github.com/authzed/spicedb/issues/3433).
package poolsim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/authzed/spicedb/internal/goroutineid"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ErrNestedAcquire is returned when a goroutine that already holds a simulated
// connection requests another one.
var ErrNestedAcquire = errors.New("nested connection acquisition: goroutine already holds a pooled connection")

// ErrPoolExhausted is returned when a simulated connection could not be acquired
// within the configured acquire timeout.
var ErrPoolExhausted = errors.New("connection pool exhausted: timed out waiting for a pooled connection")

// Option configures the simulated pool.
type Option func(*Datastore)

// WithAcquireTimeout sets how long an acquisition waits for a free connection
// before failing with ErrPoolExhausted. Real SpiceDB reads detach the caller's
// cancellation before reaching the pool, so a real acquisition never gives up;
// the timeout exists only so that tests fail instead of hanging.
func WithAcquireTimeout(timeout time.Duration) Option {
	return func(d *Datastore) { d.acquireTimeout = timeout }
}

// WithAllowNestedAcquire disables failing fast when a goroutine that already
// holds a connection requests another. Nested acquisitions are still counted, and
// will block on the pool like a real one would.
func WithAllowNestedAcquire() Option {
	return func(d *Datastore) { d.failOnNested = false }
}

// Datastore is a datastore proxy that models a fixed-size connection pool for
// all reads made via SnapshotReader.
type Datastore struct {
	datastore.Datastore

	slots          chan struct{}
	acquireTimeout time.Duration
	failOnNested   bool

	heldMu sync.Mutex
	held   map[uint64]int

	nestedCount  atomic.Int64
	timeoutCount atomic.Int64
	inUse        atomic.Int64
	maxInUse     atomic.Int64
}

// New wraps the delegate datastore with a simulated connection pool of the given
// size.
func New(delegate datastore.Datastore, size int, opts ...Option) *Datastore {
	if size < 1 {
		panic("poolsim: pool size must be at least 1")
	}

	d := &Datastore{
		Datastore:      delegate,
		slots:          make(chan struct{}, size),
		acquireTimeout: 10 * time.Second,
		failOnNested:   true,
		held:           map[uint64]int{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// NestedAcquisitions returns the number of times a goroutine that already held
// a connection requested another.
func (d *Datastore) NestedAcquisitions() int64 { return d.nestedCount.Load() }

// AcquireTimeouts returns the number of acquisitions that timed out.
func (d *Datastore) AcquireTimeouts() int64 { return d.timeoutCount.Load() }

// MaxInUse returns the high-water mark of simultaneously held connections.
func (d *Datastore) MaxInUse() int64 { return d.maxInUse.Load() }

// InUse returns the number of connections currently held.
func (d *Datastore) InUse() int64 { return d.inUse.Load() }

func (d *Datastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &reader{d.Datastore.SnapshotReader(rev), d}
}

func (d *Datastore) Unwrap() datastore.Datastore {
	return d.Datastore
}

func (d *Datastore) acquire(operation string) (func(), error) {
	gid := goroutineid.Current()

	d.heldMu.Lock()
	alreadyHeld := d.held[gid]
	d.heldMu.Unlock()

	if alreadyHeld > 0 {
		d.nestedCount.Add(1)
		if d.failOnNested {
			return nil, fmt.Errorf("%w (operation %s)", ErrNestedAcquire, operation)
		}
	}

	timer := time.NewTimer(d.acquireTimeout)
	defer timer.Stop()

	select {
	case d.slots <- struct{}{}:
	case <-timer.C:
		d.timeoutCount.Add(1)
		return nil, fmt.Errorf("%w (operation %s, waited %s)", ErrPoolExhausted, operation, d.acquireTimeout)
	}

	inUse := d.inUse.Add(1)
	for {
		current := d.maxInUse.Load()
		if inUse <= current || d.maxInUse.CompareAndSwap(current, inUse) {
			break
		}
	}

	d.heldMu.Lock()
	d.held[gid]++
	d.heldMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			d.heldMu.Lock()
			d.held[gid]--
			if d.held[gid] == 0 {
				delete(d.held, gid)
			}
			d.heldMu.Unlock()

			d.inUse.Add(-1)
			<-d.slots
		})
	}, nil
}

type reader struct {
	delegate datastore.Reader
	d        *Datastore
}

func withConn[T any](d *Datastore, operation string, f func() (T, error)) (T, error) {
	release, err := d.acquire(operation)
	if err != nil {
		return *new(T), err
	}
	defer release()
	return f()
}

func (r *reader) CountRelationships(ctx context.Context, name string) (int, error) {
	return withConn(r.d, "CountRelationships", func() (int, error) {
		return r.delegate.CountRelationships(ctx, name)
	})
}

func (r *reader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return withConn(r.d, "LookupCounters", func() ([]datastore.RelationshipCounter, error) {
		return r.delegate.LookupCounters(ctx)
	})
}

type definitionAndRevision[T any] struct {
	def T
	rev datastore.Revision
}

func (r *reader) LegacyReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	result, err := withConn(r.d, "LegacyReadCaveatByName", func() (definitionAndRevision[*core.CaveatDefinition], error) {
		def, rev, err := r.delegate.LegacyReadCaveatByName(ctx, name)
		return definitionAndRevision[*core.CaveatDefinition]{def, rev}, err
	})
	return result.def, result.rev, err
}

func (r *reader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return withConn(r.d, "LegacyListAllCaveats", func() ([]datastore.RevisionedCaveat, error) {
		return r.delegate.LegacyListAllCaveats(ctx)
	})
}

func (r *reader) LegacyLookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	return withConn(r.d, "LegacyLookupCaveatsWithNames", func() ([]datastore.RevisionedCaveat, error) {
		return r.delegate.LegacyLookupCaveatsWithNames(ctx, names)
	})
}

func (r *reader) LegacyReadNamespaceByName(ctx context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
	result, err := withConn(r.d, "LegacyReadNamespaceByName", func() (definitionAndRevision[*core.NamespaceDefinition], error) {
		def, rev, err := r.delegate.LegacyReadNamespaceByName(ctx, name)
		return definitionAndRevision[*core.NamespaceDefinition]{def, rev}, err
	})
	return result.def, result.rev, err
}

func (r *reader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return withConn(r.d, "LegacyListAllNamespaces", func() ([]datastore.RevisionedNamespace, error) {
		return r.delegate.LegacyListAllNamespaces(ctx)
	})
}

func (r *reader) LegacyLookupNamespacesWithNames(ctx context.Context, names []string) ([]datastore.RevisionedNamespace, error) {
	return withConn(r.d, "LegacyLookupNamespacesWithNames", func() ([]datastore.RevisionedNamespace, error) {
		return r.delegate.LegacyLookupNamespacesWithNames(ctx, names)
	})
}

func (r *reader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	return withConn(r.d, "ReadStoredSchema", func() (*datastore.ReadOnlyStoredSchema, error) {
		return r.delegate.ReadStoredSchema(ctx)
	})
}

func (r *reader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	it, err := r.delegate.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	return r.holdingIterator("QueryRelationships", it), nil
}

func (r *reader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	it, err := r.delegate.ReverseQueryRelationships(ctx, subjectsFilter, opts...)
	if err != nil {
		return nil, err
	}
	return r.holdingIterator("ReverseQueryRelationships", it), nil
}

// holdingIterator models the SQL datastores, which execute the query when
// iteration begins and hold the connection until iteration ends, including
// while the consumer's loop body runs.
func (r *reader) holdingIterator(operation string, it datastore.RelationshipIterator) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		release, err := r.d.acquire(operation)
		if err != nil {
			yield(tuple.Relationship{}, err)
			return
		}
		defer release()

		for rel, err := range it {
			if !yield(rel, err) {
				return
			}
		}
	}
}

var (
	_ datastore.Datastore            = (*Datastore)(nil)
	_ datastore.UnwrappableDatastore = (*Datastore)(nil)
	_ datastore.Reader               = (*reader)(nil)
)
