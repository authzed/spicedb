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
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	defaultWatchBufferLength = 128
	numAttempts              = 10
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
				revision: revisionFromTimestamp(time.Now().UTC()).Decimal,
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
	revision.DecimalDecoder

	db             *memdb.MemDB
	revisions      []snapshot
	activeWriteTxn *memdb.Txn

	negativeGCWindow   decimal.Decimal
	quantizationPeriod decimal.Decimal
	watchBufferLength  uint16
	uniqueID           string
}

type snapshot struct {
	revision decimal.Decimal
	db       *memdb.MemDB
}

func (mdb *memdbDatastore) SnapshotReader(revisionRaw datastore.Revision) datastore.Reader {
	dr := revisionRaw.(revision.Decimal)

	mdb.RLock()
	defer mdb.RUnlock()

	if len(mdb.revisions) == 0 {
		return &memdbReader{nil, nil, fmt.Errorf("memdb datastore is not ready")}
	}

	if err := mdb.checkRevisionLocalCallerMustLock(dr); err != nil {
		return &memdbReader{nil, nil, err}
	}

	revIndex := sort.Search(len(mdb.revisions), func(i int) bool {
		return mdb.revisions[i].revision.GreaterThanOrEqual(dr.Decimal)
	})

	// handle the case when there is no revision snapshot newer than the requested revision
	if revIndex == len(mdb.revisions) {
		revIndex = len(mdb.revisions) - 1
	}

	rev := mdb.revisions[revIndex]
	if rev.db == nil {
		return &memdbReader{nil, nil, fmt.Errorf("memdb datastore is already closed")}
	}

	roTxn := rev.db.Txn(false)
	txSrc := func() (*memdb.Txn, error) {
		return roTxn, nil
	}

	return &memdbReader{noopTryLocker{}, txSrc, nil}
}

func (mdb *memdbDatastore) ReadWriteTx(
	_ context.Context,
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

		newRevision := mdb.newRevisionID()
		rwt := &memdbReadWriteTx{memdbReader{&sync.Mutex{}, txSrc, nil}, newRevision}
		if err := f(rwt); err != nil {
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
						rt, err := change.After.(*relationship).RelationTuple()
						if err != nil {
							return datastore.NoRevision, err
						}
						newChanges.Changes = append(newChanges.Changes, &corev1.RelationTupleUpdate{
							Operation: corev1.RelationTupleUpdate_TOUCH,
							Tuple:     rt,
						})
					}
					if change.After == nil && change.Before != nil {
						rt, err := change.Before.(*relationship).RelationTuple()
						if err != nil {
							return datastore.NoRevision, err
						}
						newChanges.Changes = append(newChanges.Changes, &corev1.RelationTupleUpdate{
							Operation: corev1.RelationTupleUpdate_DELETE,
							Tuple:     rt,
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
		mdb.revisions = append(mdb.revisions, snapshot{newRevision.Decimal, snap})
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

func (mdb *memdbDatastore) Features(_ context.Context) (*datastore.Features, error) {
	return &datastore.Features{Watch: datastore.Feature{Enabled: true}}, nil
}

func (mdb *memdbDatastore) Close() error {
	mdb.Lock()
	defer mdb.Unlock()

	// TODO Make this nil once we have removed all access to closed datastores
	if db := mdb.db; db != nil {
		mdb.revisions = []snapshot{
			{
				revision: revisionFromTimestamp(time.Now().UTC()).Decimal,
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
