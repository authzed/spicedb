package schemacaching

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var namespacesFallbackModeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "watching_schema_cache_namespaces_fallback_mode",
	Help:      "value of 1 if the cache is in fallback mode and 0 otherwise",
})

var caveatsFallbackModeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "watching_schema_cache_caveats_fallback_mode",
	Help:      "value of 1 if the cache is in fallback mode and 0 otherwise",
})

var schemaCacheRevisionGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "watching_schema_cache_tracked_revision",
	Help:      "the currently tracked max revision for the schema cache",
})

var definitionsReadCachedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "watching_schema_cache_definitions_read_cached_total",
	Help:      "cached number of definitions read from the watching cache",
}, []string{"definition_kind"})

var definitionsReadTotalCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "spicedb",
	Subsystem: "datastore",
	Name:      "watching_schema_cache_definitions_read_total",
	Help:      "total number of definitions read from the watching cache",
}, []string{"definition_kind"})

const maximumRetryCount = 10

func init() {
	prometheus.MustRegister(namespacesFallbackModeGauge, caveatsFallbackModeGauge, schemaCacheRevisionGauge, definitionsReadCachedCounter, definitionsReadTotalCounter)
}

// watchingCachingProxy is a datastore proxy that caches schema (namespaces and caveat definitions)
// and updates its cache via a WatchSchema call. If the supplied datastore to be wrapped does not support
// this API, or the data is not available in this case or an error occurs, the updating cache fallsback
// to the standard schema cache.
type watchingCachingProxy struct {
	datastore.Datastore

	fallbackCache  *definitionCachingProxy
	gcWindow       time.Duration
	watchHeartbeat time.Duration
	closed         chan bool

	namespaceCache *schemaWatchCache[*core.NamespaceDefinition]
	caveatCache    *schemaWatchCache[*core.CaveatDefinition]
}

// createWatchingCacheProxy creates and returns a watching cache proxy.
func createWatchingCacheProxy(delegate datastore.Datastore, c cache.Cache[cache.StringKey, *cacheEntry], gcWindow time.Duration, watchHeartbeat time.Duration) *watchingCachingProxy {
	fallbackCache := &definitionCachingProxy{
		Datastore: delegate,
		c:         c,
	}

	proxy := &watchingCachingProxy{
		Datastore:     delegate,
		fallbackCache: fallbackCache,

		gcWindow:       gcWindow,
		watchHeartbeat: watchHeartbeat,
		closed:         make(chan bool, 2),

		namespaceCache: newSchemaWatchCache[*core.NamespaceDefinition](
			"namespace",
			datastore.NewNamespaceNotFoundErr,
			func(ctx context.Context, name string, revision datastore.Revision) (*core.NamespaceDefinition, datastore.Revision, error) {
				return fallbackCache.SnapshotReader(revision).ReadNamespaceByName(ctx, name)
			},
			func(ctx context.Context, names []string, revision datastore.Revision) ([]datastore.RevisionedDefinition[*core.NamespaceDefinition], error) {
				return fallbackCache.SnapshotReader(revision).LookupNamespacesWithNames(ctx, names)
			},
			definitionsReadCachedCounter,
			definitionsReadTotalCounter,
			namespacesFallbackModeGauge,
		),
		caveatCache: newSchemaWatchCache[*core.CaveatDefinition](
			"caveat",
			datastore.NewCaveatNameNotFoundErr,
			func(ctx context.Context, name string, revision datastore.Revision) (*core.CaveatDefinition, datastore.Revision, error) {
				return fallbackCache.SnapshotReader(revision).ReadCaveatByName(ctx, name)
			},
			func(ctx context.Context, names []string, revision datastore.Revision) ([]datastore.RevisionedDefinition[*core.CaveatDefinition], error) {
				return fallbackCache.SnapshotReader(revision).LookupCaveatsWithNames(ctx, names)
			},
			definitionsReadCachedCounter,
			definitionsReadTotalCounter,
			caveatsFallbackModeGauge,
		),
	}
	return proxy
}

func (p *watchingCachingProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.Datastore.SnapshotReader(rev)
	return &watchingCachingReader{delegateReader, rev, p}
}

func (p *watchingCachingProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	// NOTE: we always use the standard approach cache here, as it stores changes within the transaction
	// itself, and should not impact the overall updating cache.
	return p.fallbackCache.ReadWriteTx(ctx, f, opts...)
}

func (p *watchingCachingProxy) Start(ctx context.Context) error {
	// Start async so that prepopulating doesn't block the server start.
	go func() {
		_ = p.startSync(ctx)
	}()

	return nil
}

func (p *watchingCachingProxy) startSync(ctx context.Context) error {
	log.Info().Msg("starting watching cache")
	headRev, err := p.Datastore.HeadRevision(context.Background())
	if err != nil {
		p.namespaceCache.setFallbackMode()
		p.caveatCache.setFallbackMode()
		log.Warn().Err(err).Msg("received error in schema watch")
		return err
	}

	// Start watching for expired entries to be GCed.
	go (func() {
		log.Debug().Str("revision", headRev.String()).Msg("starting watching cache GC goroutine")

		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("GC routine for watch closed due to context cancelation")
				return

			case <-p.closed:
				log.Debug().Msg("GC routine for watch closed")
				return

			case <-time.After(time.Hour):
				log.Debug().Msg("beginning GC operation for schema watch")
				p.namespaceCache.gcStaleEntries(p.gcWindow)
				p.caveatCache.gcStaleEntries(p.gcWindow)
				log.Debug().Msg("schema watch gc operation completed")
			}
		}
	})()

	var wg sync.WaitGroup
	wg.Add(1)

	// Start watching for schema changes.
	go (func() {
		retryCount := uint8(0)

	restartWatch:
		for {
			p.namespaceCache.reset()
			p.caveatCache.reset()

			log.Debug().Str("revision", headRev.String()).Msg("starting watching cache watch operation")
			reader := p.Datastore.SnapshotReader(headRev)

			// Populate the cache with all definitions at the head revision.
			log.Info().Str("revision", headRev.String()).Msg("prepopulating namespace watching cache")
			namespaces, err := reader.ListAllNamespaces(ctx)
			if err != nil {
				p.namespaceCache.setFallbackMode()
				p.caveatCache.setFallbackMode()
				log.Warn().Err(err).Msg("received error in schema watch")
				wg.Done()
				return
			}

			for _, namespaceDef := range namespaces {
				err := p.namespaceCache.updateDefinition(namespaceDef.Definition.Name, namespaceDef.Definition, false, headRev)
				if err != nil {
					p.namespaceCache.setFallbackMode()
					p.caveatCache.setFallbackMode()
					log.Warn().Err(err).Msg("received error in schema watch")
					wg.Done()
					return
				}
			}
			log.Info().Str("revision", headRev.String()).Int("count", len(namespaces)).Msg("populated namespace watching cache")

			log.Info().Str("revision", headRev.String()).Msg("prepopulating caveat watching cache")
			caveats, err := reader.ListAllCaveats(ctx)
			if err != nil {
				p.namespaceCache.setFallbackMode()
				p.caveatCache.setFallbackMode()
				log.Warn().Err(err).Msg("received error in schema watch")
				wg.Done()
				return
			}

			for _, caveatDef := range caveats {
				err := p.caveatCache.updateDefinition(caveatDef.Definition.Name, caveatDef.Definition, false, headRev)
				if err != nil {
					p.namespaceCache.setFallbackMode()
					p.caveatCache.setFallbackMode()
					log.Warn().Err(err).Msg("received error in schema watch")
					wg.Done()
					return
				}
			}
			log.Info().Str("revision", headRev.String()).Int("count", len(caveats)).Msg("populated caveat watching cache")

			log.Debug().Str("revision", headRev.String()).Dur("watch-heartbeat", p.watchHeartbeat).Msg("beginning schema watch")
			ssc, serrc := p.Datastore.Watch(ctx, headRev, datastore.WatchOptions{
				Content:            datastore.WatchSchema | datastore.WatchCheckpoints,
				CheckpointInterval: p.watchHeartbeat,
			})
			spiceerrors.DebugAssertNotNil(ssc, "ssc is nil")
			spiceerrors.DebugAssertNotNil(serrc, "serrc is nil")

			log.Debug().Msg("schema watch started")

			p.namespaceCache.startAtRevision(headRev)
			p.caveatCache.startAtRevision(headRev)

			wg.Done()

			for {
				select {
				case <-ctx.Done():
					log.Debug().Msg("schema watch closed due to context cancelation")
					return

				case <-p.closed:
					log.Debug().Msg("schema watch closed")
					return

				case ss := <-ssc:
					log.Trace().
						Bool("is-checkpoint", ss.IsCheckpoint).
						Int("changed-definition-count", len(ss.ChangedDefinitions)).
						Int("deleted-namespace-count", len(ss.DeletedNamespaces)).
						Int("deleted-caveat-count", len(ss.DeletedCaveats)).
						Msg("received update from schema watch")

					if ss.IsCheckpoint {
						if converted, ok := ss.Revision.(revisions.WithInexactFloat64); ok {
							schemaCacheRevisionGauge.Set(converted.InexactFloat64())
						}

						p.namespaceCache.setCheckpointRevision(ss.Revision)
						p.caveatCache.setCheckpointRevision(ss.Revision)
						continue
					}

					// Apply the change to the interval tree entry.
					for _, changeDef := range ss.ChangedDefinitions {
						switch t := changeDef.(type) {
						case *core.NamespaceDefinition:
							err := p.namespaceCache.updateDefinition(t.Name, t, false, ss.Revision)
							if err != nil {
								p.namespaceCache.setFallbackMode()
								log.Warn().Err(err).Msg("received error in schema watch")
							}

						case *core.CaveatDefinition:
							err := p.caveatCache.updateDefinition(t.Name, t, false, ss.Revision)
							if err != nil {
								p.caveatCache.setFallbackMode()
								log.Warn().Err(err).Msg("received error in schema watch")
							}

						default:
							p.namespaceCache.setFallbackMode()
							p.caveatCache.setFallbackMode()
							log.Error().Msg("unknown change definition type")
							return
						}
					}

					for _, deletedNamespaceName := range ss.DeletedNamespaces {
						err := p.namespaceCache.updateDefinition(deletedNamespaceName, nil, true, ss.Revision)
						if err != nil {
							p.namespaceCache.setFallbackMode()
							log.Warn().Err(err).Msg("received error in schema watch")
							break
						}
					}

					for _, deletedCaveatName := range ss.DeletedCaveats {
						err := p.caveatCache.updateDefinition(deletedCaveatName, nil, true, ss.Revision)
						if err != nil {
							p.caveatCache.setFallbackMode()
							log.Warn().Err(err).Msg("received error in schema watch")
							break
						}
					}

				case err := <-serrc:
					var retryable datastore.WatchRetryableError
					if errors.As(err, &retryable) && retryCount <= maximumRetryCount {
						log.Warn().Err(err).Msg("received retryable error in schema watch; sleeping for a bit and restarting watch")
						retryCount++
						wg.Add(1)
						pgxcommon.SleepOnErr(ctx, err, retryCount)
						continue restartWatch
					}

					p.namespaceCache.setFallbackMode()
					p.caveatCache.setFallbackMode()
					log.Warn().Err(err).Msg("received terminal error in schema watch; setting to permanent fallback mode")
					return
				}
			}
		}
	})()

	wg.Wait()
	return nil
}

func (p *watchingCachingProxy) Close() error {
	p.caveatCache.setFallbackMode()
	p.namespaceCache.setFallbackMode()

	// Close both goroutines
	p.closed <- true
	p.closed <- true

	return errors.Join(p.fallbackCache.Close(), p.Datastore.Close())
}

// schemaWatchCache is a schema cache which updates based on changes received via the WatchSchema
// call.
type schemaWatchCache[T datastore.SchemaDefinition] struct {
	// kind is a descriptive label of the kind of definitions in the cache.
	kind string

	notFoundError     notFoundErrorFn
	readDefinition    readDefinitionFn[T]
	lookupDefinitions lookupDefinitionsFn[T]

	// inFallbackMode, if true, indicates that an error occurred with the WatchSchema call and that
	// all further calls to this cache should passthrough, rather than using the cache itself (which
	// is likely out of date).
	// *Must* be accessed under the lock.
	inFallbackMode bool

	// checkpointRevision is the current revision at which the cache has been given *all* possible
	// changes.
	// *Must* be accessed under the lock.
	checkpointRevision datastore.Revision

	// entries are the entries in the cache, by name of the namespace or caveat.
	// *Must* be accessed under the lock.
	entries map[string]*intervalTracker[revisionedEntry[T]]

	// definitionsReadCachedCounter is a counter of the number of cached definitions
	// returned by the cache directly (without fallback)
	definitionsReadCachedCounter *prometheus.CounterVec

	// definitionsReadTotalCounter is a counter of the total number of definitions
	// returned.
	definitionsReadTotalCounter *prometheus.CounterVec

	// fallbackGauge is a gauge holding a value of whether the cache is in fallback mode.
	fallbackGauge prometheus.Gauge

	lock sync.RWMutex
}

type revisionedEntry[T datastore.SchemaDefinition] struct {
	revisionedDefinition datastore.RevisionedDefinition[T]
	wasNotFound          bool
}

type (
	notFoundErrorFn                                   func(name string) error
	readDefinitionFn[T datastore.SchemaDefinition]    func(ctx context.Context, name string, revision datastore.Revision) (T, datastore.Revision, error)
	lookupDefinitionsFn[T datastore.SchemaDefinition] func(ctx context.Context, names []string, revision datastore.Revision) ([]datastore.RevisionedDefinition[T], error)
)

// newSchemaWatchCache creates a new schema watch cache, starting in fallback mode.
// To bring out of fallback mode, call startAtRevision to indicate that a watch loop
// has begun at that revision.
func newSchemaWatchCache[T datastore.SchemaDefinition](
	kind string,
	notFoundError notFoundErrorFn,
	readDefinition readDefinitionFn[T],
	lookupDefinitions lookupDefinitionsFn[T],
	definitionsReadCachedCounter *prometheus.CounterVec,
	definitionsReadTotalCounter *prometheus.CounterVec,
	fallbackGauge prometheus.Gauge,
) *schemaWatchCache[T] {
	fallbackGauge.Set(1)

	return &schemaWatchCache[T]{
		kind: kind,

		notFoundError:     notFoundError,
		readDefinition:    readDefinition,
		lookupDefinitions: lookupDefinitions,

		inFallbackMode:     true,
		entries:            map[string]*intervalTracker[revisionedEntry[T]]{},
		checkpointRevision: nil,

		lock: sync.RWMutex{},

		definitionsReadCachedCounter: definitionsReadCachedCounter,
		definitionsReadTotalCounter:  definitionsReadTotalCounter,
		fallbackGauge:                fallbackGauge,
	}
}

func (swc *schemaWatchCache[T]) startAtRevision(revision datastore.Revision) {
	swc.lock.Lock()
	defer swc.lock.Unlock()

	swc.checkpointRevision = revision
	swc.inFallbackMode = false

	swc.fallbackGauge.Set(0)
}

func (swc *schemaWatchCache[T]) gcStaleEntries(gcWindow time.Duration) {
	swc.lock.Lock()
	defer swc.lock.Unlock()

	for entryName, entry := range swc.entries {
		fullyRemoved := entry.removeStaleIntervals(gcWindow)
		if fullyRemoved {
			delete(swc.entries, entryName)
		}
	}
}

func (swc *schemaWatchCache[T]) setFallbackMode() {
	swc.lock.Lock()
	defer swc.lock.Unlock()

	swc.inFallbackMode = true
	swc.fallbackGauge.Set(1)
}

func (swc *schemaWatchCache[T]) reset() {
	swc.lock.Lock()
	defer swc.lock.Unlock()

	swc.inFallbackMode = false
	swc.fallbackGauge.Set(0)
	swc.entries = map[string]*intervalTracker[revisionedEntry[T]]{}
	swc.checkpointRevision = nil
}

func (swc *schemaWatchCache[T]) setCheckpointRevision(revision datastore.Revision) {
	swc.lock.Lock()
	defer swc.lock.Unlock()

	swc.checkpointRevision = revision
}

func (swc *schemaWatchCache[T]) getTrackerForName(name string) *intervalTracker[revisionedEntry[T]] {
	swc.lock.RLock()
	tracker, ok := swc.entries[name]
	swc.lock.RUnlock()

	if ok {
		return tracker
	}

	tracker = newIntervalTracker[revisionedEntry[T]]()
	swc.lock.Lock()
	swc.entries[name] = tracker
	swc.lock.Unlock()
	return tracker
}

func (swc *schemaWatchCache[T]) updateDefinition(name string, definition T, isDeletion bool, revision datastore.Revision) error {
	tracker := swc.getTrackerForName(name)
	result := tracker.add(revisionedEntry[T]{
		revisionedDefinition: datastore.RevisionedDefinition[T]{
			Definition:          definition,
			LastWrittenRevision: revision,
		},
		wasNotFound: isDeletion,
	}, revision)
	if !result {
		return spiceerrors.MustBugf("received out of order insertion for definition %s", name)
	}
	return nil
}

func (swc *schemaWatchCache[T]) readDefinitionByName(ctx context.Context, name string, revision datastore.Revision) (T, datastore.Revision, error) {
	swc.definitionsReadTotalCounter.WithLabelValues(swc.kind).Inc()

	swc.lock.RLock()
	inFallbackMode := swc.inFallbackMode
	lastCheckpointRevision := swc.checkpointRevision
	swc.lock.RUnlock()

	// If in fallback mode, just read the definition directly from the fallback cache.
	if inFallbackMode {
		return swc.readDefinition(ctx, name, revision)
	}

	// Lookup the tracker for the definition name and then find the associated definition for the specified revision,
	// if any.
	tracker := swc.getTrackerForName(name)
	found, ok := tracker.lookup(revision, lastCheckpointRevision)
	if ok {
		swc.definitionsReadCachedCounter.WithLabelValues(swc.kind).Inc()

		// If an entry was found, return the stored information.
		if found.wasNotFound {
			return *new(T), nil, swc.notFoundError(name)
		}

		return found.revisionedDefinition.Definition, found.revisionedDefinition.LastWrittenRevision, nil
	}

	// Otherwise, read the definition from the fallback cache.
	return swc.readDefinition(ctx, name, revision)
}

func (swc *schemaWatchCache[T]) readDefinitionsWithNames(ctx context.Context, names []string, revision datastore.Revision) ([]datastore.RevisionedDefinition[T], error) {
	swc.definitionsReadTotalCounter.WithLabelValues(swc.kind).Add(float64(len(names)))

	swc.lock.RLock()
	inFallbackMode := swc.inFallbackMode
	lastCheckpointRevision := swc.checkpointRevision
	swc.lock.RUnlock()

	// If in fallback mode, just read the definition directly from the fallback cache.
	if inFallbackMode {
		return swc.lookupDefinitions(ctx, names, revision)
	}

	// Find whichever trackers are cached.
	remainingNames := mapz.NewSet(names...)
	foundDefs := make([]datastore.RevisionedDefinition[T], 0, len(names))
	for _, name := range names {
		tracker := swc.getTrackerForName(name)
		found, ok := tracker.lookup(revision, lastCheckpointRevision)
		if !ok {
			continue
		}

		swc.definitionsReadCachedCounter.WithLabelValues(swc.kind).Inc()
		remainingNames.Delete(name)
		if !found.wasNotFound {
			foundDefs = append(foundDefs, found.revisionedDefinition)
		}
	}

	// If there are still remaining definition names to be looked up, look them up and then cache them.
	if !remainingNames.IsEmpty() {
		additionalDefs, err := swc.lookupDefinitions(ctx, remainingNames.AsSlice(), revision)
		if err != nil {
			return nil, err
		}

		foundDefs = append(foundDefs, additionalDefs...)
	}

	return foundDefs, nil
}

type watchingCachingReader struct {
	datastore.Reader
	rev datastore.Revision
	p   *watchingCachingProxy
}

func (r *watchingCachingReader) ReadNamespaceByName(
	ctx context.Context,
	name string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	return r.p.namespaceCache.readDefinitionByName(ctx, name, r.rev)
}

func (r *watchingCachingReader) LookupNamespacesWithNames(
	ctx context.Context,
	nsNames []string,
) ([]datastore.RevisionedNamespace, error) {
	return r.p.namespaceCache.readDefinitionsWithNames(ctx, nsNames, r.rev)
}

func (r *watchingCachingReader) ReadCaveatByName(
	ctx context.Context,
	name string,
) (*core.CaveatDefinition, datastore.Revision, error) {
	return r.p.caveatCache.readDefinitionByName(ctx, name, r.rev)
}

func (r *watchingCachingReader) LookupCaveatsWithNames(
	ctx context.Context,
	caveatNames []string,
) ([]datastore.RevisionedCaveat, error) {
	return r.p.caveatCache.readDefinitionsWithNames(ctx, caveatNames, r.rev)
}
