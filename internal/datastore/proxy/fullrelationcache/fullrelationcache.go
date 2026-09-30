package fullrelationcache

import (
	"context"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"
	"resenje.org/singleflight"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	metricsNamespace = "spicedb"
	metricsSubsystem = "full_relation_cache"

	tooBigMemoCost = 64

	// tooBigMemoReprobeInterval is the age at which the proxy ignores a too-big memo.
	// This re-probes a hot object that became smaller than the size cap.
	// The access-based expiry of the cache never evicts a hot memo.
	tooBigMemoReprobeInterval = time.Minute

	// maxConcurrentMaterializations limits the concurrent materializing reads of one QueryRelationships call.
	maxConcurrentMaterializations = 8
)

var (
	hitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "hits_total",
		Help:      "resource IDs served from a complete cached set",
	})
	missesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "misses_total",
		Help:      "resource IDs with no cached set or memo at the requested revision",
	})
	negativeAnswersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "negative_answers_total",
		Help:      "resource IDs answered definitively-empty from a complete set with no datastore contact",
	})
	materializationsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "materializations_total",
		Help:      "complete sets read from the datastore and stored",
	})
	tooBigTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "too_big_total",
		Help:      "materialization attempts that exceeded the maximum set size and stored a too-big memo",
	})
	promotionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "promotions_total",
		Help:      "object#relations whose access count reached the materialize threshold",
	})
	setSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "set_size",
		Help:      "number of relationships in each materialized complete set",
		Buckets:   prometheus.ExponentialBuckets(1, 4, 8),
	})
)

// Options configures the full relation cache proxy.
type Options struct {
	// MaterializeThreshold is the access count at one revision that materializes an object#relation at that revision.
	MaterializeThreshold uint64

	// MaximumSetSize is the largest set that the proxy stores. A larger set gets a too-big memo.
	MaximumSetSize uint64

	// NowFunc gives the time for serve-time expiration and too-big memo age. nil means time.Now.
	NowFunc func() time.Time
}

type proxy struct {
	datastore.Datastore

	sets    cache.Cache[SetKey, *CachedSet]
	counter AccessCounter
	group   singleflight.Group[string, *CachedSet]
	opts    Options
}

var (
	_ datastore.Datastore            = (*proxy)(nil)
	_ datastore.UnwrappableDatastore = (*proxy)(nil)
)

// NewProxy returns a datastore that serves snapshot queries for a hot object#relation from complete sets.
// The caller owns sets and counter.
func NewProxy(delegate datastore.Datastore, sets cache.Cache[SetKey, *CachedSet],
	counter AccessCounter, opts Options,
) datastore.Datastore {
	if opts.NowFunc == nil {
		opts.NowFunc = time.Now
	}
	return &proxy{Datastore: delegate, sets: sets, counter: counter, opts: opts}
}

// SnapshotReader returns a reader that serves queries at rev from complete sets where it can.
func (p *proxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &setCacheReader{
		Reader: p.Datastore.SnapshotReader(rev),
		p:      p,
		rev:    rev.String(),
	}
}

// Unwrap returns the delegate datastore.
func (p *proxy) Unwrap() datastore.Datastore { return p.Datastore }

// ReadWriteTx comes from the delegate. A transaction has no fixed revision, so it never uses sets.

type setCacheReader struct {
	datastore.Reader

	p   *proxy
	rev string
}

// servable reports whether complete sets can answer a query.
// Other queries go to the delegate.
func servable(filter datastore.RelationshipsFilter, opts *options.QueryOptions) bool {
	return filter.IsServableFromCompleteSet() && opts.ReturnsAllUnordered()
}

// QueryRelationships serves each resource ID from a complete set if possible.
// One delegate query reads the other IDs.
// Hot IDs without a set materialize concurrently.
//
// The remainder query runs after the iterator yields the served relationships.
// Thus its error comes during iteration, and a consumer that stops early never runs it.
func (r *setCacheReader) QueryRelationships(ctx context.Context,
	filter datastore.RelationshipsFilter, queryOpts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	opts := options.NewQueryOptionsWithOptions(queryOpts...)
	if !servable(filter, opts) {
		log.Ctx(ctx).Trace().
			Str("resource_type", filter.OptionalResourceType).
			Str("relation", filter.OptionalResourceRelation).
			Int("resource_id_count", len(filter.OptionalResourceIds)).
			Msg("full relation cache: query is not servable, passing through")
		return r.Reader.QueryRelationships(ctx, filter, queryOpts...)
	}

	resourceType, relation := filter.OptionalResourceType, filter.OptionalResourceRelation
	if slices.Contains(filter.OptionalResourceIds, "") {
		return nil, spiceerrors.MustBugf("full relation cache: empty resource ID in a query for %s#%s", resourceType, relation)
	}

	objectIDs := uniqueIDs(filter.OptionalResourceIds)
	sets := make([]*CachedSet, len(objectIDs))
	var hot []int
	for i, objectID := range objectIDs {
		set, materialize := r.lookup(ctx, resourceType, objectID, relation)
		sets[i] = set
		if materialize {
			hot = append(hot, i)
		}
	}
	r.materializeAll(ctx, resourceType, relation, objectIDs, hot, sets)

	var served []tuple.Relationship
	remainder := make([]string, 0, len(objectIDs))
	now := r.p.opts.NowFunc()
	for i, set := range sets {
		if set == nil {
			remainder = append(remainder, objectIDs[i])
			continue
		}
		hitsTotal.Inc()
		matched := serveFromSet(set, filter, *opts, now)
		if len(matched) == 0 {
			negativeAnswersTotal.Inc()
		}
		served = append(served, matched...)
	}

	if len(remainder) == len(objectIDs) {
		return r.Reader.QueryRelationships(ctx, filter, queryOpts...)
	}

	return func(yield func(tuple.Relationship, error) bool) {
		for _, rel := range served {
			if !yield(rel, nil) {
				return
			}
		}
		if len(remainder) == 0 {
			return
		}
		subFilter := filter
		subFilter.OptionalResourceIds = remainder
		it, err := r.Reader.QueryRelationships(ctx, subFilter, queryOpts...)
		if err != nil {
			yield(tuple.Relationship{}, err)
			return
		}
		for rel, err := range it {
			if !yield(rel, err) {
				return
			}
		}
	}, nil
}

// uniqueIDs removes duplicate ids and keeps the first-seen order, so the proxy never serves a set twice for one query.
// It presizes its map. BenchmarkUniqueIDs measures a mapz.Set version as about 2x slower.
func uniqueIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// lookup returns the complete set for the object at this revision.
// If there is no set, it reports whether to materialize the object.
//
// The counter key is the set key, so hotness is per revision.
// Reads that each use a new revision never materialize, because no set is reusable.
func (r *setCacheReader) lookup(ctx context.Context, resourceType, objectID, relation string) (*CachedSet, bool) {
	key := NewSetKey(resourceType, objectID, relation, r.rev)
	if set, ok := r.p.sets.Get(key); ok && set.Complete {
		log.Ctx(ctx).Trace().Str("set_key", string(key)).Int("set_size", len(set.Rels)).
			Msg("full relation cache: hit")
		return set, false
	}
	if r.hasFreshTooBigMemo(resourceType, objectID, relation) {
		log.Ctx(ctx).Trace().Str("set_key", string(key)).
			Msg("full relation cache: too-big memo hit, passing through")
		return nil, false
	}
	missesTotal.Inc()

	count := r.p.counter.Touch(string(key))
	if count < r.p.opts.MaterializeThreshold {
		log.Ctx(ctx).Trace().Str("set_key", string(key)).
			Uint64("access_count", count).Uint64("materialize_threshold", r.p.opts.MaterializeThreshold).
			Msg("full relation cache: miss below the materialize threshold")
		return nil, false
	}
	if count == r.p.opts.MaterializeThreshold {
		promotionsTotal.Inc()
	}
	return nil, true
}

// materializeAll materializes objectIDs[i] into sets[i] for each i in hot.
func (r *setCacheReader) materializeAll(ctx context.Context, resourceType, relation string,
	objectIDs []string, hot []int, sets []*CachedSet,
) {
	switch len(hot) {
	case 0:
		return
	case 1:
		i := hot[0]
		sets[i] = r.materializeShared(ctx, resourceType, objectIDs[i], relation)
	default:
		var g errgroup.Group
		g.SetLimit(maxConcurrentMaterializations)
		for _, i := range hot {
			g.Go(func() error {
				sets[i] = r.materializeShared(ctx, resourceType, objectIDs[i], relation)
				return nil
			})
		}
		// The goroutines never return errors. A failure sends the ID to the delegate.
		_ = g.Wait()
	}
}

// materializeShared returns a complete set for the object through a singleflight per set key.
// It returns nil if the set is too big or the read fails. A cache failure never fails the query.
//
// The singleflight context is independent of the cancellation of each caller.
// A waiter whose context ends gets ctx.Err() and does not affect the other waiters.
func (r *setCacheReader) materializeShared(ctx context.Context,
	resourceType, objectID, relation string,
) *CachedSet {
	key := NewSetKey(resourceType, objectID, relation, r.rev)
	set, _, err := r.p.group.Do(ctx, string(key), func(ctx context.Context) (*CachedSet, error) {
		if set, ok := r.p.sets.Get(key); ok {
			return set, nil
		}
		if r.hasFreshTooBigMemo(resourceType, objectID, relation) {
			return nil, nil
		}
		return r.materialize(ctx, key, resourceType, objectID, relation)
	})
	if err != nil {
		// The query reads this object from the datastore instead, so the error does not fail it.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			log.Ctx(ctx).Debug().Str("set_key", string(key)).Err(err).
				Msg("full relation cache: materialization stopped; reading from the datastore")
		} else {
			log.Ctx(ctx).Warn().Str("set_key", string(key)).Err(err).
				Msg("full relation cache: materialization failed; reading from the datastore")
		}
		return nil
	}
	if set == nil || !set.Complete {
		return nil
	}
	return set
}

// hasFreshTooBigMemo reports whether a too-big memo younger than tooBigMemoReprobeInterval exists.
// The proxy ignores an older memo, and the next probe overwrites it.
func (r *setCacheReader) hasFreshTooBigMemo(resourceType, objectID, relation string) bool {
	memo, ok := r.p.sets.Get(NewTooBigMemoKey(resourceType, objectID, relation))
	return ok && r.p.opts.NowFunc().Sub(memo.MemoizedAt) < tooBigMemoReprobeInterval
}

// materialize reads the object#relation without a subject filter and with LIMIT cap+1.
// Within the cap, it stores and returns a complete set.
// Over the cap, it stores a too-big memo and returns nil.
// A memo only sends queries to the delegate, so it is valid at all revisions.
func (r *setCacheReader) materialize(ctx context.Context, key SetKey,
	resourceType, objectID, relation string,
) (*CachedSet, error) {
	queryOpts := []options.QueryOptionsOption{
		options.WithQueryShape(queryshape.AllSubjectsForResources),
	}
	if r.p.opts.MaximumSetSize < math.MaxUint64 {
		limit := r.p.opts.MaximumSetSize + 1
		queryOpts = append(queryOpts, options.WithLimit(&limit))
	}

	log.Ctx(ctx).Trace().Str("set_key", string(key)).Uint64("maximum_set_size", r.p.opts.MaximumSetSize).
		Msg("full relation cache: materialize start")
	rels, err := r.readAll(ctx, resourceType, objectID, relation, queryOpts)
	if err != nil {
		log.Ctx(ctx).Trace().Str("set_key", string(key)).Err(err).
			Msg("full relation cache: materialize failed, passing through")
		return nil, err
	}

	if uint64(len(rels)) > r.p.opts.MaximumSetSize {
		r.p.sets.Set(NewTooBigMemoKey(resourceType, objectID, relation),
			&CachedSet{Complete: false, MemoizedAt: r.p.opts.NowFunc()}, tooBigMemoCost)
		tooBigTotal.Inc()
		log.Ctx(ctx).Trace().Str("set_key", string(key)).
			Int("read_count", len(rels)).Uint64("maximum_set_size", r.p.opts.MaximumSetSize).
			Msg("full relation cache: materialize found a too-big set, stored a memo")
		return nil, nil
	}

	// Only a read without truncation and without a subject filter can be complete.
	set := &CachedSet{Complete: true, Rels: rels}
	r.p.sets.Set(key, set, set.Cost())
	materializationsTotal.Inc()
	setSize.Observe(float64(len(rels)))
	log.Ctx(ctx).Trace().Str("set_key", string(key)).Int("set_size", len(rels)).
		Msg("full relation cache: materialize stored a complete set")
	return set, nil
}

// readAll reads every relationship of the object#relation from the delegate.
func (r *setCacheReader) readAll(ctx context.Context, resourceType, objectID, relation string,
	queryOpts []options.QueryOptionsOption,
) ([]tuple.Relationship, error) {
	it, err := r.Reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     resourceType,
		OptionalResourceIds:      []string{objectID},
		OptionalResourceRelation: relation,
	}, queryOpts...)
	if err != nil {
		return nil, err
	}
	return datastore.IteratorToSlice(it)
}
