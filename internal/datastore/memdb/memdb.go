package memdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"
	"github.com/shopspring/decimal"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
)

// DisableGC is a convenient constant for setting the garbage collection
// interval high enough that it will never run.
const DisableGC = time.Duration(math.MaxInt64)

const (
	tableRelationship = "relationship"
	tableTransaction  = "transaction"
	tableNamespace    = "namespaceConfig"

	indexID                         = "id"
	indexUnique                     = "unique"
	indexTimestamp                  = "timestamp"
	indexLive                       = "live"
	indexNamespace                  = "namespace"
	indexNamespaceAndResourceID     = "namespaceAndResourceID"
	indexNamespaceAndRelation       = "namespaceAndRelation"
	indexNamespaceAndSubjectID      = "namespaceAndSubjectID"
	indexSubjectNamespace           = "subjectNamespace"
	indexFullSubject                = "subject"
	indexSubjectAndResourceRelation = "subjectAndResourceRelation"
	indexCreatedTxn                 = "createdTxn"
	indexDeletedTxn                 = "deletedTxn"

	defaultWatchBufferLength = 128

	deletedTransactionID = ^uint64(0)

	errUnableToInstantiateTuplestore = "unable to instantiate datastore: %w"
)

type hasLifetime interface {
	getCreatedTxn() uint64
	getDeletedTxn() uint64
}

type namespace struct {
	name        string
	configBytes []byte
	createdTxn  uint64
	deletedTxn  uint64
}

func (ns namespace) getCreatedTxn() uint64 {
	return ns.createdTxn
}

func (ns namespace) getDeletedTxn() uint64 {
	return ns.deletedTxn
}

var _ hasLifetime = &namespace{}

type transaction struct {
	id        uint64
	timestamp uint64
}

type relationship struct {
	namespace        string
	resourceID       string
	relation         string
	subjectNamespace string
	subjectObjectID  string
	subjectRelation  string
	createdTxn       uint64
	deletedTxn       uint64
}

func (r relationship) getCreatedTxn() uint64 {
	return r.createdTxn
}

func (r relationship) getDeletedTxn() uint64 {
	return r.deletedTxn
}

var _ hasLifetime = &relationship{}

func tupleEntryFromRelationship(r *v1.Relationship, created, deleted uint64) *relationship {
	return &relationship{
		namespace:        r.Resource.ObjectType,
		resourceID:       r.Resource.ObjectId,
		relation:         r.Relation,
		subjectNamespace: r.Subject.Object.ObjectType,
		subjectObjectID:  r.Subject.Object.ObjectId,
		subjectRelation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
		createdTxn:       created,
		deletedTxn:       deleted,
	}
}

func (r relationship) Relationship() *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: r.namespace,
			ObjectId:   r.resourceID,
		},
		Relation: r.relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: r.subjectNamespace,
				ObjectId:   r.subjectObjectID,
			},
			OptionalRelation: stringz.Default(r.subjectRelation, "", datastore.Ellipsis),
		},
	}
}

func (r relationship) RelationTuple() *core.RelationTuple {
	return &core.RelationTuple{
		ObjectAndRelation: &core.ObjectAndRelation{
			Namespace: r.namespace,
			ObjectId:  r.resourceID,
			Relation:  r.relation,
		},
		User: &core.User{UserOneof: &core.User_Userset{Userset: &core.ObjectAndRelation{
			Namespace: r.subjectNamespace,
			ObjectId:  r.subjectObjectID,
			Relation:  r.subjectRelation,
		}}},
	}
}

func (r relationship) String() string {
	return fmt.Sprintf(
		"%s:%s#%s@%s:%s#%s[%d-%d)",
		r.namespace,
		r.resourceID,
		r.relation,
		r.subjectNamespace,
		r.subjectObjectID,
		r.subjectRelation,
		r.createdTxn,
		r.deletedTxn,
	)
}

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		tableNamespace: {
			Name: tableNamespace,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:   indexID,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "name"},
						},
					},
				},
				indexUnique: {
					Name:   indexUnique,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "name"},
							&memdb.UintFieldIndex{Field: "createdTxn"},
						},
					},
				},
				indexLive: {
					Name:   indexLive,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "name"},
							&memdb.UintFieldIndex{Field: "deletedTxn"},
						},
					},
				},
				indexDeletedTxn: {
					Name:    indexDeletedTxn,
					Unique:  false,
					Indexer: &memdb.UintFieldIndex{Field: "deletedTxn"},
				},
			},
		},
		tableTransaction: {
			Name: tableTransaction,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.UintFieldIndex{Field: "id"},
				},
				indexTimestamp: {
					Name:    indexTimestamp,
					Unique:  false,
					Indexer: &memdb.UintFieldIndex{Field: "timestamp"},
				},
			},
		},
		tableRelationship: {
			Name: tableRelationship,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:   indexID,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "resourceID"},
							&memdb.StringFieldIndex{Field: "relation"},
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
							&memdb.UintFieldIndex{Field: "createdTxn"},
						},
					},
				},
				indexLive: {
					Name:   indexLive,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "resourceID"},
							&memdb.StringFieldIndex{Field: "relation"},
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
							&memdb.UintFieldIndex{Field: "deletedTxn"},
						},
					},
				},
				indexNamespace: {
					Name:    indexNamespace,
					Unique:  false,
					Indexer: &memdb.StringFieldIndex{Field: "namespace"},
				},
				indexNamespaceAndResourceID: {
					Name:   indexNamespaceAndResourceID,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "resourceID"},
						},
					},
				},
				indexNamespaceAndRelation: {
					Name:   indexNamespaceAndRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
				},
				indexNamespaceAndSubjectID: {
					Name:   indexNamespaceAndSubjectID,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
						},
					},
				},
				indexSubjectNamespace: {
					Name:   indexSubjectNamespace,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
						},
					},
				},
				indexFullSubject: {
					Name:   indexFullSubject,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
						},
					},
				},
				indexSubjectAndResourceRelation: {
					Name:   indexSubjectAndResourceRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
				},
				indexCreatedTxn: {
					Name:    indexCreatedTxn,
					Unique:  false,
					Indexer: &memdb.UintFieldIndex{Field: "createdTxn"},
				},
				indexDeletedTxn: {
					Name:    indexDeletedTxn,
					Unique:  false,
					Indexer: &memdb.UintFieldIndex{Field: "deletedTxn"},
				},
			},
		},
	},
}

type memdbDatastore struct {
	sync.RWMutex
	db                       *memdb.MemDB
	watchBufferLength        uint16
	revisionFuzzingTimedelta time.Duration
	gcWindowInverted         time.Duration
	simulatedLatency         time.Duration
}

// NewMemdbDatastore creates a new Datastore compliant datastore backed by memdb.
//
// If the watchBufferLength value of 0 is set then a default value of 128 will be used.
func NewMemdbDatastore(
	watchBufferLength uint16,
	revisionFuzzingTimedelta,
	gcWindow time.Duration,
	simulatedLatency time.Duration,
) (datastore.Datastore, error) {
	if revisionFuzzingTimedelta > gcWindow {
		return nil, fmt.Errorf(
			errUnableToInstantiateTuplestore,
			errors.New("gc window must be larger than fuzzing window"),
		)
	}

	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiateTuplestore, err)
	}

	txn := db.Txn(true)
	defer txn.Abort()

	// Add a changelog entry to make the first revision non-zero, matching the other datastore
	// implementations.
	_, err = createNewTransaction(txn)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiateTuplestore, err)
	}

	txn.Commit()

	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	return &memdbDatastore{
		db:                       db,
		watchBufferLength:        watchBufferLength,
		revisionFuzzingTimedelta: revisionFuzzingTimedelta,

		gcWindowInverted: -1 * gcWindow,
		simulatedLatency: simulatedLatency,
	}, nil
}

func (mds *memdbDatastore) IsReady(ctx context.Context) (bool, error) {
	return true, nil
}

func (mds *memdbDatastore) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	return fmt.Sprintf("%s@%s", namespaceName, revision), nil
}

func revisionFromVersion(version uint64) datastore.Revision {
	return decimal.NewFromInt(int64(version))
}

func (mds *memdbDatastore) Close() error {
	mds.Lock()
	mds.db = nil
	mds.Unlock()
	return nil
}

func createNewTransaction(txn *memdb.Txn) (uint64, error) {
	var newTransactionID uint64 = 1

	lastChangeRaw, err := txn.Last(tableTransaction, indexID)
	if err != nil {
		return 0, err
	}

	if lastChangeRaw != nil {
		newTransactionID = lastChangeRaw.(*transaction).id + 1
	}

	newChangelogEntry := &transaction{
		id:        newTransactionID,
		timestamp: uint64(time.Now().UnixNano()),
	}

	if err := txn.Insert(tableTransaction, newChangelogEntry); err != nil {
		return 0, err
	}

	return newTransactionID, nil
}

// filterToLiveObjects creates a memdb.FilterFunc which returns true for the items to remove,
// which is opposite of most filter implementations.
func filterToLiveObjects(revision datastore.Revision) memdb.FilterFunc {
	return func(hasLifetimeRaw interface{}) bool {
		obj := hasLifetimeRaw.(hasLifetime)
		return uint64(revision.IntPart()) < obj.getCreatedTxn() || uint64(revision.IntPart()) >= obj.getDeletedTxn()
	}
}
