package crdb

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/common/changebuffer"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	diskBufferAccumulations = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "crdb_watch_disk_buffer_accumulations_total",
		Help:      "total number of changes accumulated to disk buffer",
	})

	diskBufferCheckpoints = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore",
		Name:      "crdb_watch_disk_buffer_checkpoints_total",
		Help:      "total number of checkpoints processed from disk buffer",
	})
)

// diskBufferedChangeProvider accumulates changes to disk until checkpoint.
// It implements the changeTracker interface for watch streaming.
type diskBufferedChangeProvider struct {
	store         *changebuffer.Store
	content       datastore.WatchContent
	sendChange    sendChangeFunc
	sendError     sendErrorFunc
	checkpointRev *revisions.HLCRevision
	firstRev      *revisions.HLCRevision
	lastRev       *revisions.HLCRevision
}

func newDiskBufferedChangeProvider(
	db *pebble.DB,
	content datastore.WatchContent,
	sendChange sendChangeFunc,
	sendError sendErrorFunc,
) (*diskBufferedChangeProvider, error) {
	// Wrap ParseRevisionString to match changebuffer.ParsingFunc signature
	parsingFunc := func(revisionStr string) (datastore.Revision, error) {
		return ParseRevisionString(revisionStr)
	}

	store, err := changebuffer.New(parsingFunc, db)
	if err != nil {
		return nil, err
	}

	return &diskBufferedChangeProvider{
		store:      store,
		content:    content,
		sendChange: sendChange,
		sendError:  sendError,
	}, nil
}

// FilterAndRemoveRevisionChanges implements the changeTracker interface.
// When a checkpoint is reached, it iterates over all accumulated changes and yields them.
func (d *diskBufferedChangeProvider) FilterAndRemoveRevisionChanges(
	_ func(lhs, rhs revisions.HLCRevision) bool,
	boundRev revisions.HLCRevision,
) ([]datastore.RevisionChanges, error) {
	// A checkpoint was received - set it and we'll process in the next call
	if d.checkpointRev == nil {
		d.checkpointRev = &boundRev
		return nil, nil
	}

	// Process accumulated changes up to the checkpoint
	ctx := context.Background()

	log.Debug().
		Str("checkpoint_rev", d.checkpointRev.String()).
		Msg("processing disk buffered changes at checkpoint")

	var changes []datastore.RevisionChanges
	for ch, err := range d.store.IterateRevisionChangesUpToBoundary(ctx, *d.checkpointRev) {
		if err != nil {
			log.Err(err).Msg("error iterating revision changes from disk buffer")
			return nil, err
		}

		// Track first and last revisions for cleanup
		rev := ch.Revision.(revisions.HLCRevision)
		if d.firstRev == nil || rev.LessThan(*d.firstRev) {
			d.firstRev = &rev
		}
		if d.lastRev == nil || rev.GreaterThan(*d.lastRev) {
			d.lastRev = &rev
		}

		// Convert changebuffer.RevisionChanges to datastore.RevisionChanges
		dsChanges := datastore.RevisionChanges{
			Revision:            ch.Revision,
			RelationshipChanges: ch.RelationshipChanges,
			ChangedDefinitions:  ch.ChangedDefinitions,
			DeletedNamespaces:   ch.DeletedNamespaces,
			DeletedCaveats:      ch.DeletedCaveats,
			Metadatas:           ch.Metadatas, // Already in the correct format
		}

		changes = append(changes, dsChanges)
	}

	diskBufferCheckpoints.Inc()

	// Clean up processed revisions
	if d.firstRev != nil && d.lastRev != nil {
		if err := d.store.DeleteRevisionsRange(ctx, *d.firstRev, *d.lastRev); err != nil {
			log.Warn().Err(err).Msg("failed to delete processed revisions from disk buffer")
		}
	}

	// Reset for next checkpoint
	d.checkpointRev = nil
	d.firstRev = nil
	d.lastRev = nil

	return changes, nil
}

func (d *diskBufferedChangeProvider) AddRelationshipChange(ctx context.Context, rev revisions.HLCRevision, rel tuple.Relationship, op tuple.UpdateOperation) error {
	if d.content&datastore.WatchRelationships != datastore.WatchRelationships {
		return nil
	}

	diskBufferAccumulations.Inc()

	return d.store.AddRelationshipChange(ctx, rev, rel, op)
}

func (d *diskBufferedChangeProvider) AddChangedDefinition(ctx context.Context, rev revisions.HLCRevision, def datastore.SchemaDefinition) error {
	if d.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	diskBufferAccumulations.Inc()

	return d.store.AddChangedDefinition(ctx, rev, def)
}

func (d *diskBufferedChangeProvider) AddDeletedNamespace(ctx context.Context, rev revisions.HLCRevision, namespaceName string) error {
	if d.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	diskBufferAccumulations.Inc()

	return d.store.AddDeletedNamespace(ctx, rev, namespaceName)
}

func (d *diskBufferedChangeProvider) AddDeletedCaveat(ctx context.Context, rev revisions.HLCRevision, caveatName string) error {
	if d.content&datastore.WatchSchema != datastore.WatchSchema {
		return nil
	}

	diskBufferAccumulations.Inc()

	return d.store.AddDeletedCaveat(ctx, rev, caveatName)
}

func (d *diskBufferedChangeProvider) AddRevisionMetadata(ctx context.Context, rev revisions.HLCRevision, metadata common.TransactionMetadata) error {
	if len(metadata) == 0 {
		return nil
	}

	// Convert map[string]any to *structpb.Struct before storing
	structMetadata, err := structpb.NewStruct(metadata)
	if err != nil {
		return err
	}

	return d.store.AddRevisionMetadata(ctx, rev, structMetadata)
}
