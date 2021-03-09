package memdb

import (
	"fmt"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToInstantiateTuplestore = "unable to instantiate datastore: %w"
	errUnableToWriteTuples           = "unable to write tuples: %w"
	errUnableToQueryTuples           = "unable to query tuples: %w"
	errRevision                      = "unable to find revision: %w"
)

const (
	tableTuple     = "tuple"
	tableChangelog = "changelog"

	indexID                   = "id"
	indexLive                 = "live"
	indexNamespace            = "namespace"
	indexNamespaceAndObjectID = "namespaceAndObjectID"
	indexNamespaceAndRelation = "namespaceAndRelation"
	indexNamespaceAndUserset  = "namespaceAndUserset"
)

const deletedTransactionID = ^uint64(0)

type memdbTupleDatastore struct {
	db                       *memdb.MemDB
	lastAllocatedChangelogID uint64
}

type tupleChangelog struct {
	id        uint64
	timestamp time.Time
	changes   []*pb.RelationTupleUpdate
}

type tupleEntry struct {
	namespace        string
	objectID         string
	relation         string
	usersetNamespace string
	usersetObjectID  string
	usersetRelation  string
	createdTxn       uint64
	deletedTxn       uint64
}

var tuplestoreSchema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		tableChangelog: {
			Name: tableChangelog,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.UintFieldIndex{Field: "id"},
				},
			},
		},
		tableTuple: {
			Name: tableTuple,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:   indexID,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "objectID"},
							&memdb.StringFieldIndex{Field: "relation"},
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetObjectID"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
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
							&memdb.StringFieldIndex{Field: "objectID"},
							&memdb.StringFieldIndex{Field: "relation"},
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetObjectID"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
							&memdb.UintFieldIndex{Field: "deletedTxn"},
						},
					},
				},
				indexNamespace: {
					Name:    indexNamespace,
					Unique:  false,
					Indexer: &memdb.StringFieldIndex{Field: "namespace"},
				},
				indexNamespaceAndObjectID: {
					Name:   indexNamespaceAndObjectID,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "objectID"},
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
				indexNamespaceAndUserset: {
					Name:   indexNamespaceAndUserset,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetObjectID"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
						},
					},
				},
			},
		},
	},
}

// NewMemdbTupleDatastore creates a new TupleDatastore compliant datastore backed by memdb.
func NewMemdbTupleDatastore() (datastore.TupleDatastore, error) {
	db, err := memdb.NewMemDB(tuplestoreSchema)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiateTuplestore, err)
	}

	return &memdbTupleDatastore{db: db, lastAllocatedChangelogID: 0}, nil
}

func (mtds *memdbTupleDatastore) WriteTuples(preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (uint64, error) {
	txn := mtds.db.Txn(true)

	// Check the preconditions
	for _, expectedTuple := range preconditions {
		found, err := findTuple(txn, expectedTuple)
		if err != nil {
			return 0, fmt.Errorf(errUnableToWriteTuples, err)
		}

		if found == nil {
			return 0, datastore.ErrPreconditionFailed
		}
	}

	// Create the changelog entry
	newChangelogID := mtds.lastAllocatedChangelogID + 1
	newChangelogEntry := &tupleChangelog{
		id:        newChangelogID,
		timestamp: time.Now(),
		changes:   mutations,
	}

	if err := txn.Insert(tableChangelog, newChangelogEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteTuples, err)
	}

	// Apply the mutations
	for _, mutation := range mutations {
		newVersion := &tupleEntry{
			namespace:        mutation.Tuple.ObjectAndRelation.Namespace,
			objectID:         mutation.Tuple.ObjectAndRelation.ObjectId,
			relation:         mutation.Tuple.ObjectAndRelation.Relation,
			usersetNamespace: mutation.Tuple.User.GetUserset().Namespace,
			usersetObjectID:  mutation.Tuple.User.GetUserset().ObjectId,
			usersetRelation:  mutation.Tuple.User.GetUserset().Relation,
			createdTxn:       newChangelogID,
			deletedTxn:       deletedTransactionID,
		}

		existing, err := findTuple(txn, mutation.Tuple)
		if err != nil {
			return 0, fmt.Errorf(errUnableToWriteTuples, err)
		}

		var deletedExisting tupleEntry
		if existing != nil {
			deletedExisting = *existing
			deletedExisting.deletedTxn = newChangelogID
		}

		switch mutation.Operation {
		case pb.RelationTupleUpdate_CREATE:
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		case pb.RelationTupleUpdate_DELETE:
			if existing == nil {
				return 0, datastore.ErrPreconditionFailed
			}
			if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		case pb.RelationTupleUpdate_TOUCH:
			if existing != nil {
				if err := txn.Insert(tableTuple, &deletedExisting); err != nil {
					return 0, fmt.Errorf(errUnableToWriteTuples, err)
				}
			}
			if err := txn.Insert(tableTuple, newVersion); err != nil {
				return 0, fmt.Errorf(errUnableToWriteTuples, err)
			}
		default:
			return 0, fmt.Errorf(
				errUnableToWriteTuples,
				fmt.Errorf("unknown tuple mutation operation type: %s", mutation.Operation),
			)
		}
	}

	mtds.lastAllocatedChangelogID++
	txn.Commit()

	return newChangelogID, nil
}

func (mtds *memdbTupleDatastore) QueryTuples(namespace string, revision uint64) datastore.TupleQuery {
	return &memdbTupleQuery{
		db:        mtds.db,
		namespace: namespace,
		revision:  revision,
	}
}

func (mtds *memdbTupleDatastore) Revision() (uint64, error) {
	// Compute the current revision
	txn := mtds.db.Txn(false)
	defer txn.Abort()

	lastRaw, err := txn.Last(tableChangelog, indexID)
	if err != nil {
		return 0, fmt.Errorf(errRevision, err)
	}
	if lastRaw != nil {
		return lastRaw.(*tupleChangelog).id, nil
	}
	return 0, nil
}

func findTuple(txn *memdb.Txn, toFind *pb.RelationTuple) (*tupleEntry, error) {
	foundRaw, err := txn.First(
		tableTuple,
		indexLive,
		toFind.ObjectAndRelation.Namespace,
		toFind.ObjectAndRelation.ObjectId,
		toFind.ObjectAndRelation.Relation,
		toFind.User.GetUserset().Namespace,
		toFind.User.GetUserset().ObjectId,
		toFind.User.GetUserset().Relation,
		deletedTransactionID,
	)
	if err != nil {
		return nil, err
	}

	if foundRaw == nil {
		return nil, nil
	}

	return foundRaw.(*tupleEntry), nil
}

// Ensure that we implement the interface
var _ datastore.TupleDatastore = &memdbTupleDatastore{}
