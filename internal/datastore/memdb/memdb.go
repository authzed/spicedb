package memdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	Engine                   = "memory"
	defaultWatchBufferLength = 128
	maxRetries               = 10
)

var (
	ErrMemDBIsClosed = errors.New("datastore is closed")
	ErrSerialization = errors.New("serialization error")
)

// DisableGC is a convenient constant for setting the garbage collection
// interval high enough that it will never run.
const DisableGC = time.Duration(math.MaxInt64)

// NewMemdbDatastore creates a new Datastore compliant datastore backed by memdb.
//
// If the watchBufferLength value of 0 is set then a default value of 128 will be used.
func NewMemdbDatastore(
	watchBufferLength uint16,
	revisionQuantization,
	gcWindow time.Duration,
) (datastore.Datastore, error) {
	if revisionQuantization > gcWindow {
		return nil, errors.New("gc window must be larger than quantization interval")
	}

	if revisionQuantization <= 1 {
		revisionQuantization = 1
	}

	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}

	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	uniqueID := uuid.NewString()
	mdb := &memdbDatastore{
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.Timestamp,
		},
		db: db,
		revisions: []snapshot{
			{
				revision:   nowRevision(),
				schemaHash: "",
				// A snapshot of the still-empty database rather than the
				// database itself, so that a read at the creation revision
				// above sees the datastore as it was created rather than as it
				// is now. Every later entry likewise holds a snapshot frozen
				// at its own revision.
				db: db.Snapshot(),
			},
		},

		negativeGCWindow:        gcWindow.Nanoseconds() * -1,
		quantizationPeriod:      revisionQuantization.Nanoseconds(),
		watchBufferLength:       watchBufferLength,
		watchBufferWriteTimeout: 100 * time.Millisecond,
		uniqueID:                uniqueID,
	}
	mdb.writeTxReady = sync.NewCond(&mdb.RWMutex)
	return mdb, nil
}

type memdbDatastore struct {
	sync.RWMutex
	revisions.CommonDecoder

	// NOTE: call checkNotClosed before using
	db *memdb.MemDB // GUARDED_BY(RWMutex)
	// revisions MUST be sorted in strictly increasing revision order.
	revisions      []snapshot // GUARDED_BY(RWMutex)
	activeWriteTxn *memdb.Txn // GUARDED_BY(RWMutex)
	writeTxReady   *sync.Cond // broadcast when activeWriteTxn becomes nil

	negativeGCWindow        int64
	quantizationPeriod      int64
	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	uniqueID                string
}

type snapshot struct {
	revision   revisions.TimestampRevision
	schemaHash string
	db         *memdb.MemDB
}

func (mdb *memdbDatastore) MetricsID() (string, error) {
	return "memdb", nil
}

func (mdb *memdbDatastore) EngineName() string {
	return Engine
}

func (mdb *memdbDatastore) UniqueID(_ context.Context) (string, error) {
	return mdb.uniqueID, nil
}

// SnapshotReader returns a reader for the snapshot visible at the given
// revision: the most recent entry in mdb.revisions at or before it, located by
// binary search.
func (mdb *memdbDatastore) SnapshotReader(dr datastore.Revision) datastore.Reader {
	mdb.RLock()
	defer mdb.RUnlock()

	if err := mdb.checkNotClosed(); err != nil {
		return &memdbReader{nil, nil, err, time.Now()}
	}

	if len(mdb.revisions) == 0 {
		return &memdbReader{nil, nil, errors.New("memdb datastore is not ready"), time.Now()}
	}

	if err := mdb.checkRevisionLocalCallerMustLock(dr); err != nil {
		return &memdbReader{nil, nil, err, time.Now()}
	}

	// sort.Search finds the first snapshot newer than the requested revision,
	// so the one visible at it is the entry before that.
	revIndex := sort.Search(len(mdb.revisions), func(i int) bool {
		return mdb.revisions[i].revision.GreaterThan(dr)
	})

	// Handle the case where every snapshot is newer than the requested
	// revision, i.e. it predates the datastore itself: the oldest snapshot is
	// the closest thing to the state at that revision.
	if revIndex > 0 {
		revIndex--
	}

	rev := mdb.revisions[revIndex]
	if rev.db == nil {
		return &memdbReader{nil, nil, errors.New("memdb datastore is already closed"), time.Now()}
	}

	roTxn := rev.db.Txn(false)
	txSrc := func() (*memdb.Txn, error) {
		return roTxn, nil
	}

	return &memdbReader{noopTryLocker{}, txSrc, nil, time.Now()}
}

func (mdb *memdbDatastore) getCurrentSchemaHashNoLock() string {
	txn := mdb.db.Txn(false)
	defer txn.Abort()

	raw, err := txn.First(tableSchemaRevision, indexID, "current")
	if err != nil || raw == nil {
		return ""
	}

	srd, ok := raw.(*schemaRevisionData)
	if !ok {
		return ""
	}

	return string(srd.hash)
}

func (mdb *memdbDatastore) SupportsIntegrity() bool {
	return true
}

func (mdb *memdbDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	config := options.NewRWTOptionsWithOptions(opts...)
	txNumAttempts := maxRetries // TODO every other datastore has a configurable MaxRetries. why not this one?
	if config.DisableRetries {
		txNumAttempts = 1
	}

	for i := 0; i < txNumAttempts; i++ {
		var tx *memdb.Txn
		var newRevision revisions.TimestampRevision
		rwt := &memdbReadWriteTx{}
		createTxOnce := sync.Once{}
		txSrc := func() (*memdb.Txn, error) {
			var err error
			createTxOnce.Do(func() {
				mdb.Lock()
				defer mdb.Unlock()

				// Block until any active write transaction finishes rather than
				// returning ErrSerialization and busy-retrying with sleeps.
				for mdb.activeWriteTxn != nil {
					mdb.writeTxReady.Wait()
				}

				if err = mdb.checkNotClosed(); err != nil {
					return
				}

				tx = mdb.db.Txn(true)
				tx.TrackChanges()
				mdb.activeWriteTxn = tx

				// Assign the transaction's revision now, while holding the exclusive write transaction.
				// Writers are serialized from this point until the revision is appended at commit.
				newRevision = mdb.newRevisionIDNoLock()
				rwt.newRevision = newRevision
			})

			return tx, err
		}
		rwt.memdbReader = memdbReader{&sync.Mutex{}, txSrc, nil, time.Now()}
		if config.SchemaHashPrecondition != "" {
			if err := assertSchemaHash(ctx, rwt, config.SchemaHashPrecondition); err != nil {
				mdb.Lock()
				if tx != nil {
					tx.Abort()
					mdb.activeWriteTxn = nil
					mdb.writeTxReady.Signal()
				}
				mdb.Unlock()
				return datastore.NoRevision, err
			}
		}
		if err := f(ctx, rwt); err != nil {
			mdb.Lock()
			if tx != nil {
				tx.Abort()
				mdb.activeWriteTxn = nil
				mdb.writeTxReady.Signal()
			}

			// If the error was a serialization error, retry the transaction.
			// We *must* return the inner error unmodified in case it's not an error type
			// that supports unwrapping (e.g. gRPC errors)
			if errors.Is(err, ErrSerialization) {
				mdb.Unlock()
				continue
			}
			defer mdb.Unlock()
			return datastore.NoRevision, err
		}

		mdb.Lock()
		defer mdb.Unlock() // TODO is this defer correct? it runs at the end of the function, not at the end of the for loop's iteration

		// The user function never used the transaction: nothing was written,
		// so no new revision is created and the head revision is returned.
		if tx == nil {
			if err := mdb.checkNotClosed(); err != nil {
				return datastore.NoRevision, err
			}
			return mdb.headRevisionNoLock(), nil
		}

		tracked := common.NewChanges(revisions.TimestampIDKeyFunc, datastore.WatchRelationships|datastore.WatchSchema, 0)
		if config.Metadata != nil && len(config.Metadata.GetFields()) > 0 {
			if err := tracked.AddRevisionMetadata(ctx, newRevision, config.Metadata.AsMap()); err != nil {
				return datastore.NoRevision, err
			}
		}

		for _, change := range tx.Changes() {
			switch change.Table {
			case tableRelationship:
				switch {
				case change.After != nil:
					rt, err := change.After.(*relationship).Relationship()
					if err != nil {
						return datastore.NoRevision, err
					}

					if err := tracked.AddRelationshipChange(ctx, newRevision, rt, tuple.UpdateOperationTouch); err != nil {
						return datastore.NoRevision, err
					}
				case change.After == nil && change.Before != nil:
					rt, err := change.Before.(*relationship).Relationship()
					if err != nil {
						return datastore.NoRevision, err
					}

					if err := tracked.AddRelationshipChange(ctx, newRevision, rt, tuple.UpdateOperationDelete); err != nil {
						return datastore.NoRevision, err
					}
				default:
					return datastore.NoRevision, spiceerrors.MustBugf("unexpected relationship change")
				}
			case tableNamespace:
				switch {
				case change.After != nil:
					loaded := &corev1.NamespaceDefinition{}
					if err := loaded.UnmarshalVT(change.After.(*namespace).configBytes); err != nil {
						return datastore.NoRevision, err
					}

					err := tracked.AddChangedDefinition(ctx, newRevision, loaded)
					if err != nil {
						return datastore.NoRevision, err
					}
				case change.After == nil && change.Before != nil:
					err := tracked.AddDeletedNamespace(ctx, newRevision, change.Before.(*namespace).name)
					if err != nil {
						return datastore.NoRevision, err
					}
				default:
					return datastore.NoRevision, spiceerrors.MustBugf("unexpected namespace change")
				}
			case tableCaveats:
				switch {
				case change.After != nil:
					loaded := &corev1.CaveatDefinition{}
					if err := loaded.UnmarshalVT(change.After.(*caveat).definition); err != nil {
						return datastore.NoRevision, err
					}

					err := tracked.AddChangedDefinition(ctx, newRevision, loaded)
					if err != nil {
						return datastore.NoRevision, err
					}
				case change.After == nil && change.Before != nil:
					err := tracked.AddDeletedCaveat(ctx, newRevision, change.Before.(*caveat).name)
					if err != nil {
						return datastore.NoRevision, err
					}
				default:
					return datastore.NoRevision, spiceerrors.MustBugf("unexpected namespace change")
				}
			}
		}

		changes := tracked.AsRevisionChanges(revisions.TimestampIDKeyLessThanFunc)
		wroteChangelog := false
		for rc, err := range changes {
			if err != nil {
				return datastore.NoRevision, err
			}

			if wroteChangelog {
				return datastore.NoRevision, spiceerrors.MustBugf("unexpected MemDB transaction with multiple revision changes")
			}

			change := &changelog{
				revisionNanos: newRevision.TimestampNanoSec(),
				changes:       rc,
			}
			if err := tx.Insert(tableChangelog, change); err != nil {
				return datastore.NoRevision, fmt.Errorf("error writing changelog: %w", err)
			}

			wroteChangelog = true
		}

		// Always emit a changelog entry for the committed revision, even
		// when the transaction produced no observable changes (e.g., a
		// TOUCH that matched the existing relationship). The changes
		// payload is intentionally empty — the watch goroutine constructs the
		// checkpoint event itself based on each consumer's options.
		if !wroteChangelog {
			change := &changelog{
				revisionNanos: newRevision.TimestampNanoSec(),
				changes:       datastore.RevisionChanges{},
			}
			if err := tx.Insert(tableChangelog, change); err != nil {
				return datastore.NoRevision, fmt.Errorf("error writing changelog: %w", err)
			}
		}

		tx.Commit()
		mdb.activeWriteTxn = nil
		mdb.writeTxReady.Signal()

		if err := mdb.checkNotClosed(); err != nil {
			return datastore.NoRevision, err
		}

		// Create a snapshot and add it to the revisions slice
		schemaHash := mdb.getCurrentSchemaHashNoLock()
		snap := mdb.db.Snapshot()
		mdb.revisions = append(mdb.revisions, snapshot{newRevision, schemaHash, snap})
		return newRevision, nil
	}

	return datastore.NoRevision, NewSerializationMaxRetriesReachedErr(errors.New("serialization max retries exceeded; please reduce your parallel writes"))
}

func (mdb *memdbDatastore) ReadyState(_ context.Context) (datastore.ReadyState, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	return datastore.ReadyState{
		Message: "missing expected initial revision",
		IsReady: len(mdb.revisions) > 0,
	}, nil
}

func (mdb *memdbDatastore) OfflineFeatures() (*datastore.Features, error) {
	return &datastore.Features{
		Watch: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		IntegrityData: datastore.Feature{
			Status: datastore.FeatureSupported,
		},
		ContinuousCheckpointing: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
		WatchEmitsImmediately: datastore.Feature{
			Status: datastore.FeatureUnsupported,
		},
	}, nil
}

func (mdb *memdbDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return mdb.OfflineFeatures()
}

func (mdb *memdbDatastore) Close() error {
	mdb.Lock()
	defer mdb.Unlock()

	if db := mdb.db; db != nil {
		mdb.revisions = []snapshot{
			{
				revision:   nowRevision(),
				schemaHash: "",
				db:         db,
			},
		}
	} else {
		mdb.revisions = []snapshot{}
	}

	mdb.db = nil

	return nil
}

// This code assumes that the RWMutex has been acquired.
func (mdb *memdbDatastore) checkNotClosed() error {
	if mdb.db == nil {
		return ErrMemDBIsClosed
	}
	return nil
}

var _ datastore.Datastore = &memdbDatastore{}
