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
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	defaultWatchBufferLength = 128
	numRetries               = 10
)

var errSerialization = errors.New("serialization error")

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

	negativeGCWindow := decimal.NewFromInt(gcWindow.Nanoseconds()).Mul(decimal.NewFromInt(-1))

	return &memdbDatastore{
		db: db,
		revisions: []snapshot{
			{
				revision: decimal.Zero,
				db:       db,
			},
		},

		negativeGCWindow:   negativeGCWindow,
		quantizationPeriod: decimal.NewFromInt(revisionQuantization.Nanoseconds()),
		watchBufferLength:  watchBufferLength,
		uniqueID:           uniqueID,
	}, nil
}

type memdbDatastore struct {
	sync.RWMutex

	db             *memdb.MemDB
	revisions      []snapshot
	activeWriteTxn *memdb.Txn

	negativeGCWindow   datastore.Revision
	quantizationPeriod datastore.Revision
	watchBufferLength  uint16
	uniqueID           string
}

type snapshot struct {
	revision datastore.Revision
	db       *memdb.MemDB
}

func (mdb *memdbDatastore) SnapshotReader(revision datastore.Revision) datastore.Reader {
	mdb.RLock()
	defer mdb.RUnlock()

	if len(mdb.revisions) == 0 {
		return &memdbReader{nil, nil, datastore.NoRevision, fmt.Errorf("memdb datastore is not ready")}
	}

	if err := mdb.checkRevisionLocal(revision); err != nil {
		return &memdbReader{nil, nil, datastore.NoRevision, err}
	}

	revIndex := sort.Search(len(mdb.revisions), func(i int) bool {
		return mdb.revisions[i].revision.GreaterThanOrEqual(revision)
	})

	// handle the case when there is no revision snapshot newer than the requested revision
	if revIndex == len(mdb.revisions) {
		revIndex = len(mdb.revisions) - 1
	}

	rev := mdb.revisions[revIndex]
	if rev.db == nil {
		return &memdbReader{nil, nil, datastore.NoRevision, fmt.Errorf("memdb datastore is already closed")}
	}

	snapshotRevision := rev.revision
	roTxn := rev.db.Txn(false)

	txSrc := func() (*memdb.Txn, error) {
		return roTxn, nil
	}

	return &memdbReader{noopTryLocker{}, txSrc, snapshotRevision, nil}
}

func (mdb *memdbDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (datastore.Revision, error) {
	for i := 0; i < numRetries; i++ {
		var tx *memdb.Txn
		createTxOnce := sync.Once{}
		txSrc := func() (*memdb.Txn, error) {
			var err error
			createTxOnce.Do(func() {
				mdb.Lock()
				defer mdb.Unlock()

				if mdb.activeWriteTxn != nil {
					err = errSerialization
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

		newRevision := revisionFromTimestamp(time.Now().UTC())

		rwt := &memdbReadWriteTx{memdbReader{&sync.Mutex{}, txSrc, datastore.NoRevision, nil}, newRevision}
		if err := f(ctx, rwt); err != nil {
			mdb.Lock()
			if tx != nil {
				tx.Abort()
				mdb.activeWriteTxn = nil
			}

			// If the error was a serialization error, retry the transaction
			if errors.Is(err, errSerialization) {
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

		// Record the changes that were made
		newChanges := datastore.RevisionChanges{
			Revision: newRevision,
			Changes:  nil,
		}
		if tx != nil {
			for _, change := range tx.Changes() {
				if change.Table == tableRelationship {
					if change.After != nil {
						newChanges.Changes = append(newChanges.Changes, &corev1.RelationTupleUpdate{
							Operation: corev1.RelationTupleUpdate_TOUCH,
							Tuple:     change.After.(*relationship).RelationTuple(),
						})
					}
					if change.After == nil && change.Before != nil {
						newChanges.Changes = append(newChanges.Changes, &corev1.RelationTupleUpdate{
							Operation: corev1.RelationTupleUpdate_DELETE,
							Tuple:     change.Before.(*relationship).RelationTuple(),
						})
					}
				}
			}

			change := &changelog{
				revisionNanos: newRevision.IntPart(),
				changes:       newChanges,
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

	return datastore.NoRevision, errors.New("serialization max retries exceeded")
}

func (mdb *memdbDatastore) IsReady(ctx context.Context) (bool, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	return len(mdb.revisions) > 0, nil
}

func (mdb *memdbDatastore) Features(ctx context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (mdb *memdbDatastore) Close() error {
	mdb.Lock()
	defer mdb.Unlock()

	// TODO Make this nil once we have removed all access to closed datastores
	if db := mdb.db; db != nil {
		mdb.revisions = []snapshot{
			{
				revision: decimal.Zero,
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
