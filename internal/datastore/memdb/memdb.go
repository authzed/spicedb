package memdb

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/shopspring/decimal"
)

const DisableGC = time.Duration(math.MaxInt64)

const (
	tableTuple              = "tuple"
	tableChangelog          = "changelog"
	tableNamespaceChangelog = "namespaceChangelog"
	tableNamespaceConfig    = "namespaceConfig"

	indexID                   = "id"
	indexTimestamp            = "timestamp"
	indexLive                 = "live"
	indexNamespace            = "namespace"
	indexNamespaceAndObjectID = "namespaceAndObjectID"
	indexNamespaceAndRelation = "namespaceAndRelation"
	indexNamespaceAndUserset  = "namespaceAndUserset"
	indexRelationAndUserset   = "relationAndUserset"
	indexRelationAndRelation  = "relationAndRelation"
	indexUsersetRelation      = "usersetRelation"
	indexUserset              = "userset"

	defaultWatchBufferLength = 128

	errUnableToInstantiateTuplestore = "unable to instantiate datastore: %w"
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
	timestamp uint64
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
				indexTimestamp: {
					Name:    indexTimestamp,
					Unique:  false,
					Indexer: &memdb.UintFieldIndex{Field: "timestamp"},
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
				indexRelationAndUserset: {
					Name:   indexRelationAndUserset,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetObjectID"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
				},
				indexUsersetRelation: {
					Name:   indexUsersetRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
						},
					},
				},
				indexUserset: {
					Name:   indexUserset,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetObjectID"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
						},
					},
				},
				indexRelationAndRelation: {
					Name:   indexRelationAndRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "usersetNamespace"},
							&memdb.StringFieldIndex{Field: "usersetRelation"},
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
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
			errors.New("gc window must be large than fuzzing window"),
		)
	}

	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiateTuplestore, err)
	}

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

func revisionFromVersion(version uint64) datastore.Revision {
	return decimal.NewFromInt(int64(version))
}
