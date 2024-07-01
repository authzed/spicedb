package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewCheckingReplicatedDatastore creates a new datastore that writes to the provided primary and reads
// from the provided replicas. The replicas are chosen in a round-robin fashion. If a replica does
// not have the requested revision, the primary is used instead.
//
// NOTE: Be *very* careful when using this function. It is not safe to use this function without
// knowledge of the layout of the underlying datastore and its replicas.
//
// Replicas will be checked for the requested revision before reading from them, which means that the
// read pool for the replicas *must* point to a *stable* instance of the datastore (not a load balancer).
// That means that *each* replica node in the database must be configured as its own replica to SpiceDB,
// with each URI given distinctly.
func NewCheckingReplicatedDatastore(primary datastore.Datastore, replicas ...datastore.ReadOnlyDatastore) (datastore.Datastore, error) {
	if len(replicas) == 0 {
		log.Debug().Msg("No replicas provided, using primary as read source")
		return primary, nil
	}

	cachingReplicas := make([]datastore.ReadOnlyDatastore, 0, len(replicas))
	for _, replica := range replicas {
		cachingReplicas = append(cachingReplicas, newCachedCheckRevision(replica))
	}

	log.Debug().Int("replica-count", len(replicas)).Msg("Using replicas for reads")
	return &checkingReplicatedDatastore{
		primary,
		cachingReplicas,
		0,
	}, nil
}

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

func selectReplica[T any](replicas []T, lastReplica *uint64) T {
	if len(replicas) == 1 {
		return replicas[0]
	}

	var swapped bool
	var next uint64
	for !swapped {
		last := *lastReplica
		next = (*lastReplica + 1) % uint64(len(replicas))
		swapped = atomic.CompareAndSwapUint64(lastReplica, last, next)
	}

	log.Trace().Uint64("replica", next).Msg("choosing replica for read")
	return replicas[next]
}

type checkingReplicatedDatastore struct {
	datastore.Datastore
	replicas []datastore.ReadOnlyDatastore

	lastReplica uint64
}

// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
// Any errors establishing the reader will be returned by subsequent calls.
func (rd *checkingReplicatedDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	replica := selectReplica(rd.replicas, &rd.lastReplica)
	return &checkingStableReader{
		rev:     revision,
		replica: replica,
		primary: rd.Datastore,
	}
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
	return &strictReadReplicatedReader{
		rev:     revision,
		replica: replica,
		primary: rd.Datastore,
	}
}

// checkingStableReader is a reader that will check the replica for the requested revision before
// reading from it. If the replica does not have the requested revision, the primary will be used
// instead. Only supported for a stable replica within each pool.
type checkingStableReader struct {
	rev     datastore.Revision
	replica datastore.ReadOnlyDatastore
	primary datastore.Datastore

	// chosePrimaryForTest is used for testing to determine if the primary was used for the read.
	chosePrimaryForTest bool

	chosenReader datastore.Reader
	choose       sync.Once
}

func (rr *checkingStableReader) ReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, datastore.NoRevision, err
	}

	return rr.chosenReader.ReadCaveatByName(ctx, name)
}

func (rr *checkingStableReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ListAllCaveats(ctx)
}

func (rr *checkingStableReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.LookupCaveatsWithNames(ctx, names)
}

func (rr *checkingStableReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.QueryRelationships(ctx, filter, options...)
}

func (rr *checkingStableReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ReverseQueryRelationships(ctx, subjectsFilter, options...)
}

func (rr *checkingStableReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, datastore.NoRevision, err
	}

	return rr.chosenReader.ReadNamespaceByName(ctx, nsName)
}

func (rr *checkingStableReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ListAllNamespaces(ctx)
}

func (rr *checkingStableReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.LookupNamespacesWithNames(ctx, nsNames)
}

func (rr *checkingStableReader) CountRelationships(ctx context.Context, filter string) (int, error) {
	if err := rr.determineSource(ctx); err != nil {
		return 0, err
	}

	return rr.chosenReader.CountRelationships(ctx, filter)
}

func (rr *checkingStableReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	if err := rr.determineSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.LookupCounters(ctx)
}

// determineSource will choose the replica or primary to read from based on the revision, by checking
// if the replica contains the revision. If the replica does not contain the revision, the primary
// will be used instead.
func (rr *checkingStableReader) determineSource(ctx context.Context) error {
	var finalError error
	rr.choose.Do(func() {
		// If the revision is not known to the replica, use the primary instead.
		if err := rr.replica.CheckRevision(ctx, rr.rev); err != nil {
			var irr datastore.ErrInvalidRevision
			if errors.As(err, &irr) {
				if irr.Reason() == datastore.CouldNotDetermineRevision {
					log.Trace().Str("revision", rr.rev.String()).Err(err).Msg("replica does not contain the requested revision, using primary")
					rr.chosenReader = rr.primary.SnapshotReader(rr.rev)
					rr.chosePrimaryForTest = true
					return
				}
			}
			finalError = err
			return
		}
		log.Trace().Str("revision", rr.rev.String()).Msg("replica contains the requested revision")

		rr.chosenReader = rr.replica.SnapshotReader(rr.rev)
		rr.chosePrimaryForTest = false
	})

	return finalError
}

// strictReadReplicatedReader is a reader that will use the replica for reads without itself checking for
// the requested revision. If the replica does not have the requested revision, the primary will be
// used instead. This is useful when the read pool points to a load balancer that can transparently
// handle the request. In this case, the primary will be used as a fallback if the replica does not
// have the requested revision. The replica(s) supplied to this proxy *must*, therefore, have strict
// read mode enabled, to ensure the query will fail with a RevisionUnavailableError if the revision is
// not available.
type strictReadReplicatedReader struct {
	rev     datastore.Revision
	replica datastore.ReadOnlyDatastore
	primary datastore.Datastore
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

func (rr *strictReadReplicatedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	relationships, err := sr.QueryRelationships(ctx, filter, options...)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).QueryRelationships(ctx, filter, options...)
	}
	return relationships, err
}

func (rr *strictReadReplicatedReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	sr := rr.replica.SnapshotReader(rr.rev)
	relationships, err := sr.ReverseQueryRelationships(ctx, subjectsFilter, options...)
	if err != nil && errors.As(err, &common.RevisionUnavailableError{}) {
		log.Trace().Str("revision", rr.rev.String()).Msg("replica does not contain the requested revision, using primary")
		return rr.primary.SnapshotReader(rr.rev).ReverseQueryRelationships(ctx, subjectsFilter, options...)
	}
	return relationships, err
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
