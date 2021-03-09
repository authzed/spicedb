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
	errWatchError                    = "watch error: %w"
	errWatcherFellBehind             = "watcher fell behind, disconnecting"
)

const (
	tableTuple              = "tuple"
	tableChangelog          = "changelog"
	tableNamespaceChangelog = "namespaceChangelog"
	tableNamespaceConfig    = "namespaceConfig"

	indexID                   = "id"
	indexLive                 = "live"
	indexNamespace            = "namespace"
	indexNamespaceAndObjectID = "namespaceAndObjectID"
	indexNamespaceAndRelation = "namespaceAndRelation"
	indexNamespaceAndUserset  = "namespaceAndUserset"

	defaultWatchBufferLength = 128
)

type changelog struct {
	id         uint64
	name       string
	replaces   *pb.NamespaceDefinition
	oldVersion uint64
}

type namespace struct {
	name    string
	config  *pb.NamespaceDefinition
	version uint64
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

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		tableNamespaceChangelog: {
			Name: tableNamespaceChangelog,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.UintFieldIndex{Field: "id"},
				},
			},
		},
		tableNamespaceConfig: {
			Name: tableNamespaceConfig,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.StringFieldIndex{Field: "name"},
				},
			},
		},
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

type memdbDatastore struct {
	db                *memdb.MemDB
	watchBufferLength uint16
}

// NewMemdbDatastore creates a new Datastore compliant datastore backed by memdb.
//
// If the watchBufferLength value of 0 is set then a default value of 128 will be used.
func NewMemdbDatastore(watchBufferLength uint16) (datastore.Datastore, error) {
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiateTuplestore, err)
	}

	if watchBufferLength == 0 {
		watchBufferLength = defaultWatchBufferLength
	}

	return &memdbDatastore{
		db:                db,
		watchBufferLength: watchBufferLength,
	}, nil
}
