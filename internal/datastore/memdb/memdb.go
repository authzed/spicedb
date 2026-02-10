package memdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	Engine                   = "memory"
	defaultWatchBufferLength = 128
	numAttempts              = 10

	// noHashSupported is a sentinel value indicating that schema hashing is not supported
	// by the memdb datastore. memdb uses in-memory schema storage and doesn't use SQL-based
	// schema hashing like other datastores.
	noHashSupported datastore.SchemaHash = "__memdb_no_hash_support__"
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
	return &memdbDatastore{
		CommonDecoder: revisions.CommonDecoder{
			Kind: revisions.Timestamp,
		},
		db: db,
		revisions: []snapshot{
			{
				revision:   nowRevision(),
				schemaHash: noHashSupported,
				db:         db,
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

	// NOTE: call checkNotClosed before using
	db             *memdb.MemDB // GUARDED_BY(RWMutex)
	revisions      []snapshot   // GUARDED_BY(RWMutex)
	activeWriteTxn *memdb.Txn   // GUARDED_BY(RWMutex)

	negativeGCWindow        int64
	quantizationPeriod      int64
	watchBufferLength       uint16
	watchBufferWriteTimeout time.Duration
	uniqueID                string

	// Unified schema storage
	storedSchema *corev1.StoredSchema // GUARDED_BY(RWMutex)
}

type snapshot struct {
	revision   revisions.TimestampRevision
	schemaHash datastore.SchemaHash
	db         *memdb.MemDB
}

func (mdb *memdbDatastore) MetricsID() (string, error) {
	return "memdb", nil
}

func (mdb *memdbDatastore) UniqueID(_ context.Context) (string, error) {
	return mdb.uniqueID, nil
}

func (mdb *memdbDatastore) getCurrentSchemaHashNoLock() datastore.SchemaHash {
	// Read the current schema hash from the schemarevision table
	txn := mdb.db.Txn(false)
	defer txn.Abort()

	raw, err := txn.First(tableSchemaRevision, indexID, "current")
	if err != nil || raw == nil {
		return noHashSupported
	}

	schemaRev := raw.(*schemaRevisionData)
	return datastore.SchemaHash(schemaRev.hash)
}

func (mdb *memdbDatastore) SnapshotReader(dr datastore.Revision, hash datastore.SchemaHash) datastore.Reader {
	mdb.RLock()
	defer mdb.RUnlock()

	if err := mdb.checkNotClosed(); err != nil {
		return &memdbReader{nil, nil, err, time.Now(), "", mdb}
	}

	if len(mdb.revisions) == 0 {
		return &memdbReader{nil, nil, errors.New("memdb datastore is not ready"), time.Now(), "", mdb}
	}

	if err := mdb.checkRevisionLocalCallerMustLock(dr); err != nil {
		return &memdbReader{nil, nil, err, time.Now(), "", mdb}
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
		return &memdbReader{nil, nil, errors.New("memdb datastore is already closed"), time.Now(), "", mdb}
	}

	roTxn := rev.db.Txn(false)
	txSrc := func() (*memdb.Txn, error) {
		return roTxn, nil
	}

	return &memdbReader{noopTryLocker{}, txSrc, nil, time.Now(), string(hash), mdb}
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

				if err = mdb.checkNotClosed(); err != nil {
					return
				}

				tx = mdb.db.Txn(true)
				tx.TrackChanges()
				mdb.activeWriteTxn = tx
			})

			return tx, err
		}

		newRevision := mdb.newRevisionID()
		rwt := &memdbReadWriteTx{memdbReader{&sync.Mutex{}, txSrc, nil, time.Now(), string(datastore.NoSchemaHashInTransaction), mdb}, newRevision}
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

		if err := mdb.checkNotClosed(); err != nil {
			return datastore.NoRevision, err
		}

		// Create a snapshot and add it to the revisions slice
		snap := mdb.db.Snapshot()

		// Get the current schema hash
		schemaHash := mdb.getCurrentSchemaHashNoLock()

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
				schemaHash: noHashSupported,
				db:         db,
			},
		}
	} else {
		mdb.revisions = []snapshot{}
	}

	mdb.db = nil

	return nil
}

// SchemaHashReaderForTesting returns a test-only interface for reading the schema hash directly from schema_revision table.
func (mdb *memdbDatastore) SchemaHashReaderForTesting() interface {
	ReadSchemaHash(ctx context.Context) (string, error)
} {
	return &memdbSchemaHashReaderForTesting{db: mdb}
}

// SchemaHashWatcherForTesting returns a test-only interface for watching schema hash changes.
func (mdb *memdbDatastore) SchemaHashWatcherForTesting() datastore.SingleStoreSchemaHashWatcher {
	return newMemdbSchemaHashWatcher(mdb)
}

type memdbSchemaHashReaderForTesting struct {
	db *memdbDatastore
}

func (r *memdbSchemaHashReaderForTesting) ReadSchemaHash(ctx context.Context) (string, error) {
	watcher := &memdbSchemaHashWatcher{db: r.db}
	return watcher.readSchemaHash()
}

// This code assumes that the RWMutex has been acquired.
func (mdb *memdbDatastore) checkNotClosed() error {
	if mdb.db == nil {
		return ErrMemDBIsClosed
	}
	return nil
}

// readStoredSchemaInternal is an internal method for readers/transactions to access the stored schema.
// This should NOT be called directly - use readers/transactions instead.
func (mdb *memdbDatastore) readStoredSchemaInternal() (*corev1.StoredSchema, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	if err := mdb.checkNotClosed(); err != nil {
		return nil, err
	}

	if mdb.storedSchema == nil {
		return nil, datastore.ErrSchemaNotFound
	}

	// Return a copy to prevent external mutations
	return mdb.storedSchema.CloneVT(), nil
}

// writeStoredSchemaInternal is an internal method for transactions to write the stored schema.
// This should NOT be called directly - use transactions instead.
func (mdb *memdbDatastore) writeStoredSchemaInternal(schema *corev1.StoredSchema) error {
	if schema == nil {
		return errors.New("stored schema cannot be nil")
	}

	if schema.Version == 0 {
		return errors.New("stored schema version cannot be 0")
	}

	mdb.Lock()
	defer mdb.Unlock()

	if err := mdb.checkNotClosed(); err != nil {
		return err
	}

	// Store a copy to prevent external mutations
	mdb.storedSchema = schema.CloneVT()

	// Write the schema hash to the schema revision table for fast lookups
	if err := mdb.writeSchemaHashInternal(schema); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}

	return nil
}

// writeSchemaHashInternal writes the schema hash to the in-memory schema revision table
func (mdb *memdbDatastore) writeSchemaHashInternal(schema *corev1.StoredSchema) error {
	v1 := schema.GetV1()
	if v1 == nil {
		return fmt.Errorf("unsupported schema version: %d", schema.Version)
	}

	tx := mdb.db.Txn(true)
	defer tx.Abort()

	// Delete existing hash (if any)
	if existing, err := tx.First(tableSchemaRevision, indexID, "current"); err == nil && existing != nil {
		if err := tx.Delete(tableSchemaRevision, existing); err != nil {
			return fmt.Errorf("failed to delete old hash: %w", err)
		}
	}

	// Insert new hash
	revisionData := &schemaRevisionData{
		name: "current",
		hash: []byte(v1.SchemaHash),
	}

	if err := tx.Insert(tableSchemaRevision, revisionData); err != nil {
		return fmt.Errorf("failed to insert hash: %w", err)
	}

	tx.Commit()
	return nil
}

// writeLegacySchemaHashFromDefinitionsInternal writes the schema hash computed from the given definitions
func (mdb *memdbDatastore) writeLegacySchemaHashFromDefinitionsInternal(ctx context.Context, namespaces []datastore.RevisionedNamespace, caveats []datastore.RevisionedCaveat) error {
	// Build schema definitions list
	definitions := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
	for _, ns := range namespaces {
		definitions = append(definitions, ns.Definition)
	}
	for _, caveat := range caveats {
		definitions = append(definitions, caveat.Definition)
	}

	// Sort definitions by name for consistent ordering
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].GetName() < definitions[j].GetName()
	})

	// Generate schema text from definitions
	schemaText, _, err := generator.GenerateSchema(definitions)
	if err != nil {
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	// Compute schema hash (SHA256)
	hash := sha256.Sum256([]byte(schemaText))
	schemaHash := hex.EncodeToString(hash[:])

	mdb.Lock()
	defer mdb.Unlock()

	if err := mdb.checkNotClosed(); err != nil {
		return err
	}

	tx := mdb.db.Txn(true)
	defer tx.Abort()

	// Delete existing hash (if any)
	if existing, err := tx.First(tableSchemaRevision, indexID, "current"); err == nil && existing != nil {
		if err := tx.Delete(tableSchemaRevision, existing); err != nil {
			return fmt.Errorf("failed to delete old hash: %w", err)
		}
	}

	// Insert new hash
	revisionData := &schemaRevisionData{
		name: "current",
		hash: []byte(schemaHash),
	}

	if err := tx.Insert(tableSchemaRevision, revisionData); err != nil {
		return fmt.Errorf("failed to insert hash: %w", err)
	}

	tx.Commit()
	return nil
}

var _ datastore.Datastore = &memdbDatastore{}
