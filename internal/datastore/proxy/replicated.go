package proxy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewReplicatedDatastore creates a new datastore that writes to the provided primary and reads
// from the provided replicas. The replicas are chosen in a round-robin fashion. If a replica does
// not have the requested revision, the primary is used instead.
func NewReplicatedDatastore(primary datastore.Datastore, replicas ...datastore.ReadOnlyDatastore) (datastore.Datastore, error) {
	if len(replicas) == 0 {
		log.Debug().Msg("No replicas provided, using primary as read source.")
		return primary, nil
	}

	log.Debug().Int("replica-count", len(replicas)).Msg("Using replicas for reads.")
	return &replicatedDatastore{
		primary,
		replicas,
		0,
	}, nil
}

type replicatedDatastore struct {
	datastore.Datastore
	replicas []datastore.ReadOnlyDatastore

	lastReplica uint64
}

// SnapshotReader creates a read-only handle that reads the datastore at the specified revision.
// Any errors establishing the reader will be returned by subsequent calls.
func (rd *replicatedDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	// Pick a replica and check if the revision is already known to that replica
	var swapped bool
	var next uint64
	for !swapped {
		last := rd.lastReplica
		next = (rd.lastReplica + 1) % uint64(len(rd.replicas))
		swapped = atomic.CompareAndSwapUint64(&rd.lastReplica, last, next)
	}

	log.Trace().Uint64("replica", next).Msg("Choosing replica for read.")
	replica := rd.replicas[next]
	return &replicatedReader{
		rev:     revision,
		replica: replica,
		primary: rd.Datastore,
	}
}

type replicatedReader struct {
	rev     datastore.Revision
	replica datastore.ReadOnlyDatastore
	primary datastore.Datastore

	chosePrimary bool
	chosenReader datastore.Reader
	choose       sync.Once
}

func (rr *replicatedReader) ReadCaveatByName(ctx context.Context, name string) (caveat *core.CaveatDefinition, lastWritten datastore.Revision, err error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, datastore.NoRevision, err
	}

	return rr.chosenReader.ReadCaveatByName(ctx, name)
}

func (rr *replicatedReader) ListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ListAllCaveats(ctx)
}

func (rr *replicatedReader) LookupCaveatsWithNames(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.LookupCaveatsWithNames(ctx, names)
}

func (rr *replicatedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	options ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.QueryRelationships(ctx, filter, options...)
}

func (rr *replicatedReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	options ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ReverseQueryRelationships(ctx, subjectsFilter, options...)
}

func (rr *replicatedReader) ReadNamespaceByName(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, datastore.NoRevision, err
	}

	return rr.chosenReader.ReadNamespaceByName(ctx, nsName)
}

func (rr *replicatedReader) ListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.ListAllNamespaces(ctx)
}

func (rr *replicatedReader) LookupNamespacesWithNames(ctx context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if err := rr.chooseSource(ctx); err != nil {
		return nil, err
	}

	return rr.chosenReader.LookupNamespacesWithNames(ctx, nsNames)
}

func (rr *replicatedReader) chooseSource(ctx context.Context) error {
	var finalError error
	rr.choose.Do(func() {
		// If the revision is not known to the replica, use the primary instead.
		if err := rr.replica.CheckRevision(ctx, rr.rev); err != nil {
			var irr datastore.ErrInvalidRevision
			if errors.As(err, &irr) {
				if irr.Reason() == datastore.CouldNotDetermineRevision {
					log.Trace().Str("revision", rr.rev.String()).Err(err).Msg("Replica does not contain the requested revision, using primary.")
					rr.chosenReader = rr.primary.SnapshotReader(rr.rev)
					rr.chosePrimary = true
					return
				}
			}
			finalError = err
			return
		}
		log.Trace().Str("revision", rr.rev.String()).Msg("Replica contains the requested revision.")
		rr.chosenReader = rr.replica.SnapshotReader(rr.rev)
		rr.chosePrimary = false
	})
	return finalError
}
