package memdb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
)

// DisableGC is a convenient constant for setting the garbage collection
// interval high enough that it will never run.
const DisableGC = time.Duration(math.MaxInt64)

const (
	tableRelationship = "relationship"
	tableTransaction  = "transaction"
	tableNamespace    = "namespaceConfig"

	indexID                     = "id"
	indexTimestamp              = "timestamp"
	indexLive                   = "live"
	indexNamespace              = "namespace"
	indexNamespaceAndResourceID = "namespaceAndResourceID"
	indexNamespaceAndRelation   = "namespaceAndRelation"
	indexNamespaceAndSubjectID  = "namespaceAndSubjectID"
	indexRelationAndSubject     = "relationAndSubject"
	indexRelationAndRelation    = "relationAndRelation"
	indexSubjectNamespace       = "subjectNamespace"
	indexSubjectRelation        = "subjectRelation"
	indexSubject                = "subject"
	indexCreatedTxn             = "createdTxn"
	indexDeletedTxn             = "deletedTxn"

	defaultWatchBufferLength = 128

	deletedTransactionID = ^uint64(0)

	errUnableToInstantiateTuplestore = "unable to instantiate datastore: %w"
)

type namespace struct {
	name        string
	configBytes []byte
	createdTxn  uint64
	deletedTxn  uint64
}

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

func tupleEntryFromRelationship(r *v1.Relationship, created, deleted uint64) *relationship {
	return &relationship{
		namespace:        r.Resource.ObjectType,
		resourceID:       r.Resource.ObjectId,
		relation:         r.Relation,
		subjectNamespace: r.Subject.Object.ObjectType,
		subjectObjectID:  r.Subject.Object.ObjectId,
		subjectRelation:  stringz.DefaultEmpty(r.Subject.OptionalRelation, "..."),
		createdTxn:       created,
		deletedTxn:       deleted,
	}
}

func (t relationship) Relationship() *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: t.namespace,
			ObjectId:   t.resourceID,
		},
		Relation: t.relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: t.subjectNamespace,
				ObjectId:   t.subjectObjectID,
			},
			OptionalRelation: stringz.Default(t.subjectRelation, "", datastore.Ellipsis),
		},
	}
}

func (t relationship) RelationTuple() *v0.RelationTuple {
	return &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: t.namespace,
			ObjectId:  t.resourceID,
			Relation:  t.relation,
		},
		User: &v0.User{UserOneof: &v0.User_Userset{Userset: &v0.ObjectAndRelation{
			Namespace: t.subjectNamespace,
			ObjectId:  t.subjectObjectID,
			Relation:  t.subjectRelation,
		}}},
	}
}

func (t relationship) String() string {
	return fmt.Sprintf(
		"%s:%s#%s@%s:%s#%s[%d-%d)",
		t.namespace,
		t.resourceID,
		t.relation,
		t.subjectNamespace,
		t.subjectObjectID,
		t.subjectRelation,
		t.createdTxn,
		t.deletedTxn,
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
				indexRelationAndSubject: {
					Name:   indexRelationAndSubject,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
				},
				indexSubjectRelation: {
					Name:   indexSubjectRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
						},
					},
				},
				indexSubject: {
					Name:   indexSubject,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
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
				indexRelationAndRelation: {
					Name:   indexRelationAndRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
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

func revisionFromVersion(version uint64) datastore.Revision {
	return decimal.NewFromInt(int64(version))
}

func (mds *memdbDatastore) Close() error {
	mds.db = nil
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
