package memdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	Engine                   = "memory"
	defaultWatchBufferLength = 128
	numAttempts              = 10
)

var ErrSerialization = errors.New("serialization error")

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
	return &memdbDatastore{
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.Timestamp,
		},
		db: db,
		revisions: []snapshot{
			{
				revision: nowRevision(),
				db:       db,
			},
		},

		negativeGCWindow:        gcWindow.Nanoseconds() * -1,
		quantizationPeriod:      revisionQuantization.Nanoseconds(),
		watchBufferLength:       watchBufferLength,
		watchBufferWriteTimeout: 100 * time.Millisecond,
		uniqueID:                uniqueID,
	}, nil
}

type memdbDatastore struct {
	sync.RWMutex
	revisions.CommonDecoder

	db             *memdb.MemDB
	revisions      []snapshot
	activeWriteTxn *memdb.Txn

	negativeGCWindow        int64
	quantizationPeriod      int64
	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	uniqueID                string
}

type snapshot struct {
	revision revisions.TimestampRevision
	db       *memdb.MemDB
}

func (mdb *memdbDatastore) MetricsID() (string, error) {
	return "memdb", nil
}

func (mdb *memdbDatastore) SnapshotReader(dr datastore.Revision) datastore.Reader {
	mdb.RLock()
	defer mdb.RUnlock()

	if len(mdb.revisions) == 0 {
		return &memdbReader{nil, nil, fmt.Errorf("memdb datastore is not ready"), time.Now()}
	}

	if err := mdb.checkRevisionLocalCallerMustLock(dr); err != nil {
		return &memdbReader{nil, nil, err, time.Now()}
	}

	revIndex := sort.Search(len(mdb.revisions), func(i int) bool {
		return mdb.revisions[i].revision.GreaterThan(dr) || mdb.revisions[i].revision.Equal(dr)
	})

	// handle the case when there is no revision snapshot newer than the requested revision
	if revIndex == len(mdb.revisions) {
		revIndex = len(mdb.revisions) - 1
	}

	rev := mdb.revisions[revIndex]
	if rev.db == nil {
		return &memdbReader{nil, nil, fmt.Errorf("memdb datastore is already closed"), time.Now()}
	}

	roTxn := rev.db.Txn(false)
	txSrc := func() (*memdb.Txn, error) {
		return roTxn, nil
	}

	return &memdbReader{noopTryLocker{}, txSrc, nil, time.Now()}
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
	txNumAttempts := numAttempts
	if config.DisableRetries {
		txNumAttempts = 1
	}

	for i := 0; i < txNumAttempts; i++ {
		var tx *memdb.Txn
		createTxOnce := sync.Once{}
		txSrc := func() (*memdb.Txn, error) {
			var err error
			createTxOnce.Do(func() {
				mdb.Lock()
				defer mdb.Unlock()

				if mdb.activeWriteTxn != nil {
					err = ErrSerialization
					return
				}

				if mdb.db == nil {
					err = fmt.Errorf("datastore is closed")
					return
				}

				tx = mdb.db.Txn(true)
				tx.TrackChanges()
				mdb.activeWriteTxn = tx
			})

			return tx, err
		}

		newRevision := mdb.newRevisionID()
		rwt := &memdbReadWriteTx{memdbReader{&sync.Mutex{}, txSrc, nil, time.Now()}, newRevision}
		if err := f(ctx, rwt); err != nil {
			mdb.Lock()
			if tx != nil {
				tx.Abort()
				mdb.activeWriteTxn = nil
			}

			// If the error was a serialization error, retry the transaction
			if errors.Is(err, ErrSerialization) {
				mdb.Unlock()

				// If we don't sleep here, we run out of retries instantaneously
				time.Sleep(1 * time.Millisecond)
				continue
			}
			defer mdb.Unlock()

			// We *must* return the inner error unmodified in case it's not an error type
			// that supports unwrapping (e.g. gRPC errors)
			return datastore.NoRevision, err
		}

		mdb.Lock()
		defer mdb.Unlock()

		tracked := common.NewChanges(revisions.TimestampIDKeyFunc, datastore.WatchRelationships|datastore.WatchSchema, 0)
		if tx != nil {
			if config.Metadata != nil && len(config.Metadata.GetFields()) > 0 {
				if err := tracked.SetRevisionMetadata(ctx, newRevision, config.Metadata.AsMap()); err != nil {
					return datastore.NoRevision, err
				}
			}

			for _, change := range tx.Changes() {
				switch change.Table {
				case tableRelationship:
					if change.After != nil {
						rt, err := change.After.(*relationship).Relationship()
						if err != nil {
							return datastore.NoRevision, err
						}

						if err := tracked.AddRelationshipChange(ctx, newRevision, rt, tuple.UpdateOperationTouch); err != nil {
							return datastore.NoRevision, err
						}
					} else if change.After == nil && change.Before != nil {
						rt, err := change.Before.(*relationship).Relationship()
						if err != nil {
							return datastore.NoRevision, err
						}

						if err := tracked.AddRelationshipChange(ctx, newRevision, rt, tuple.UpdateOperationDelete); err != nil {
							return datastore.NoRevision, err
						}
					} else {
						return datastore.NoRevision, spiceerrors.MustBugf("unexpected relationship change")
					}
				case tableNamespace:
					if change.After != nil {
						loaded := &corev1.NamespaceDefinition{}
						if err := loaded.UnmarshalVT(change.After.(*namespace).configBytes); err != nil {
							return datastore.NoRevision, err
						}

						err := tracked.AddChangedDefinition(ctx, newRevision, loaded)
						if err != nil {
							return datastore.NoRevision, err
						}
					} else if change.After == nil && change.Before != nil {
						err := tracked.AddDeletedNamespace(ctx, newRevision, change.Before.(*namespace).name)
						if err != nil {
							return datastore.NoRevision, err
						}
					} else {
						return datastore.NoRevision, spiceerrors.MustBugf("unexpected namespace change")
					}
				case tableCaveats:
					if change.After != nil {
						loaded := &corev1.CaveatDefinition{}
						if err := loaded.UnmarshalVT(change.After.(*caveat).definition); err != nil {
							return datastore.NoRevision, err
						}

						err := tracked.AddChangedDefinition(ctx, newRevision, loaded)
						if err != nil {
							return datastore.NoRevision, err
						}
					} else if change.After == nil && change.Before != nil {
						err := tracked.AddDeletedCaveat(ctx, newRevision, change.Before.(*caveat).name)
						if err != nil {
							return datastore.NoRevision, err
						}
					} else {
						return datastore.NoRevision, spiceerrors.MustBugf("unexpected namespace change")
					}
				}
			}

			var rc datastore.RevisionChanges
			changes, err := tracked.AsRevisionChanges(revisions.TimestampIDKeyLessThanFunc)
			if err != nil {
				return datastore.NoRevision, err
			}

			if len(changes) > 1 {
				return datastore.NoRevision, spiceerrors.MustBugf("unexpected MemDB transaction with multiple revision changes")
			} else if len(changes) == 1 {
				rc = changes[0]
			}

			change := &changelog{
				revisionNanos: newRevision.TimestampNanoSec(),
				changes:       rc,
			}
			if err := tx.Insert(tableChangelog, change); err != nil {
				return datastore.NoRevision, fmt.Errorf("error writing changelog: %w", err)
			}

			tx.Commit()
		}
		mdb.activeWriteTxn = nil

		// Create a snapshot and add it to the revisions slice
		if mdb.db == nil {
			return datastore.NoRevision, fmt.Errorf("datastore has been closed")
		}

		snap := mdb.db.Snapshot()
		mdb.revisions = append(mdb.revisions, snapshot{newRevision, snap})
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
				revision: nowRevision(),
				db:       db,
			},
		}
	} else {
		mdb.revisions = []snapshot{}
	}

	mdb.db = nil

	return nil
}

var _ datastore.Datastore = &memdbDatastore{}
