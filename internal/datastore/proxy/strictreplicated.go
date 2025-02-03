package proxy

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	strictReadReplicatedTotalQueryCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_replica",
		Name:      "strict_replicated_query_total",
		Help:      "total number of reads made by the strict read replicated datastore",
	})

	strictReadReplicatedFallbackQueryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_replica",
		Name:      "strict_replicated_fallback_query_total",
		Help:      "number of queries that have fallen back to the primary datastore",
	}, []string{"replica"})
)

// NewStrictReplicatedDatastore creates a new datastore that writes to the provided primary and reads
// from the provided replicas. The replicas are chosen in a round-robin fashion. If a replica does
// not have the requested revision, the primary is used instead.
//
// Unlike NewCheckingReplicatedDatastore, this function does not check the replicas for the requested
// revision before reading from them; instead, a revision check is inserted into the SQL for each read.
// This is useful when the read pool points to a load balancer that can transparently handle the request.
// In this case, the primary will be used as a fallback if the replica does not have the requested revision.
// The replica(s) supplied to this proxy *must*, therefore, have strict read mode enabled, to ensure the
// query will fail with a RevisionUnavailableError if the revision is not available.
func NewStrictReplicatedDatastore(primary datastore.Datastore, replicas ...datastore.StrictReadDatastore) (datastore.Datastore, error) {
	if len(replicas) == 0 {
		log.Debug().Msg("No replicas provided, using primary as read source")
		return primary, nil
	}

	cachingReplicas := make([]datastore.ReadOnlyDatastore, 0, len(replicas))
	for _, replica := range replicas {
		if !replica.IsStrictReadModeEnabled() {
			return nil, fmt.Errorf("replica %v does not have strict read mode enabled", replica)
		}

		cachingReplicas = append(cachingReplicas, newCachedCheckRevision(replica))
	}

	log.Debug().Int("replica-count", len(replicas)).Msg("Using replicas for reads")
	return &strictReplicatedDatastore{
		primary,
		cachingReplicas,
		0,
	}, nil
}

type strictReplicatedDatastore struct {
	datastore.Datastore
	replicas []datastore.ReadOnlyDatastore

	lastReplica uint64
}

// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
// Any errors establishing the reader will be returned by subsequent calls.
func (rd *strictReplicatedDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	replica := selectReplica(rd.replicas, &rd.lastReplica)
	replicaID, err := replica.MetricsID()
	if err != nil {
		log.Error().Err(err).Msg("failed to get replica metrics ID")
		replicaID = "unknown"
	}

	return &strictReadReplicatedReader{
		rev:       revision,
		replica:   replica,
		replicaID: replicaID,
		primary:   rd.Datastore,
	}
}

// strictReadReplicatedReader is a reader that will use the replica for reads without itself checking for
// the requested revision. If the replica does not have the requested revision, the primary will be
// used instead. This is useful when the read pool points to a load balancer that can transparently
// handle the request. In this case, the primary will be used as a fallback if the replica does not
// have the requested revision. The replica(s) supplied to this proxy *must*, therefore, have strict
// read mode enabled, to ensure the query will fail with a RevisionUnavailableError if the revision is
// not available.
type strictReadReplicatedReader struct {
	rev       datastore.Revision
	replica   datastore.ReadOnlyDatastore
	replicaID string
	primary   datastore.Datastore
}

func (rr *strictReadReplicatedReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	caveat, lastWritten, err := sr.ReadCaveatByName(ctx, name)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("caveat", name).Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).ReadCaveatByName(ctx, name)
	}
	return caveat, lastWritten, err
}

func (rr *strictReadReplicatedReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	caveats, err := sr.ListAllCaveats(ctx)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).ListAllCaveats(ctx)
	}
	return caveats, err
}

func (rr *strictReadReplicatedReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	caveats, err := sr.LookupCaveatsWithNames(ctx, names)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).LookupCaveatsWithNames(ctx, names)
	}
	return caveats, err
}

type queryHandler[F any, O any] func(
	ctx context.Context,
	filter F,
	options ...O,
) (datastore.RelationshipIterator, error)

func queryRelationships[F any, O any](
	ctx context.Context,
	rr *strictReadReplicatedReader,
	filter F,
	options []O,
	handler func(datastore.Reader) queryHandler[F, O],
) (datastore.RelationshipIterator, error) {
	strictReadReplicatedTotalQueryCount.Inc()

	sr := rr.replica.SnapshotReader(rr.rev)
	it, err := handler(sr)(ctx, filter, options...)
	// Check for a RevisionUnavailableError, which indicates the replica does not contain the requested
	// revision. In this case, use the primary instead. This may not be returned on this call from
	// wrapped datastores that defer the actual query until the iterator is used.
	if err != nil {
		if errors.As(err, &common.RevisionUnavailableError{}) {
			log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
			strictReadReplicatedFallbackQueryCount.WithLabelValues(rr.replicaID).Inc()
			return handler(rr.primary.SnapshotReader(rr.rev))(ctx, filter, options...)
		}
		return nil, err
	}

	// PG may raise a RevisionUnavailableError if the revision is not available at any time
	// during the iteration, which means we cannot simply stream results to the parent
	// iterator and still support fallback. Therefore, we conduct a full read of all relationships
	// here, and if the iterator is exhausted without error, we return it. If an error is encountered, we
	// return the primary as a fallback.
	// TODO(jschorr): This is a temporary solution to support fallback. We should consider
	// using cursors to support fallback instead.
	rels, err := datastore.IteratorToSlice(it)
	if err != nil {
		if errors.As(err, &common.RevisionUnavailableError{}) {
			log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
			strictReadReplicatedFallbackQueryCount.WithLabelValues(rr.replicaID).Inc()
			return handler(rr.primary.SnapshotReader(rr.rev))(ctx, filter, options...)
		}
		return nil, err
	}

	return func(yield func(tuple.Relationship, error) bool) {
		for _, rel := range rels {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

func (rr *strictReadReplicatedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	return queryRelationships(ctx, rr, filter, opts,
		func(reader datastore.Reader) queryHandler[datastore.RelationshipsFilter, options.QueryOptionsOption] {
			return reader.QueryRelationships
		})
}

func (rr *strictReadReplicatedReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	return queryRelationships(ctx, rr, subjectsFilter, opts,
		func(reader datastore.Reader) queryHandler[datastore.SubjectsFilter, options.ReverseQueryOptionsOption] {
			return reader.ReverseQueryRelationships
		})
}

func (rr *strictReadReplicatedReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	namespace, lastWritten, err := sr.ReadNamespaceByName(ctx, nsName)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("namespace", nsName).Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).ReadNamespaceByName(ctx, nsName)
	}
	return namespace, lastWritten, err
}

func (rr *strictReadReplicatedReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	namespaces, err := sr.ListAllNamespaces(ctx)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).ListAllNamespaces(ctx)
	}
	return namespaces, err
}

func (rr *strictReadReplicatedReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	namespaces, err := sr.LookupNamespacesWithNames(ctx, nsNames)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).LookupNamespacesWithNames(ctx, nsNames)
	}
	return namespaces, err
}

func (rr *strictReadReplicatedReader) CountRelationships(ctx context.Context, filter string) (int, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	count, err := sr.CountRelationships(ctx, filter)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).CountRelationships(ctx, filter)
	}
	return count, err
}

func (rr *strictReadReplicatedReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	counters, err := sr.LookupCounters(ctx)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).LookupCounters(ctx)
	}
	return counters, err
}
