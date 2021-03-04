package memdb

import (
	"errors"
	"fmt"

	"github.com/hashicorp/go-memdb"

	pb "github.com/authzed/spicedb/internal/REDACTEDapi/api"
	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errUnableToInstantiate = "unable to instantiate datastore: %w"
	errUnableToWriteConfig = "unable to write namespace config: %w"
	errUnableToReadConfig  = "unable to read namespace config: %w"
)

// Publicly facing errors
var (
	ErrNotFound = errors.New("unable to find namespace")
)

type memdbNsDatastore struct {
	db               *memdb.MemDB
	lastAllocatedTxn uint64
}

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

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		"changelog": {
			Name: "changelog",
			Indexes: map[string]*memdb.IndexSchema{
				"id": {
					Name:    "id",
					Unique:  true,
					Indexer: &memdb.UintFieldIndex{Field: "id"},
				},
			},
		},
		"config": {
			Name: "config",
			Indexes: map[string]*memdb.IndexSchema{
				"id": {
					Name:   "id",
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "name"},
						},
					},
				},
			},
		},
	},
}

// NewMemdbNamespaceDatastore creates a new NamespaceDatastore compliant datastore backed by memdb.
func NewMemdbNamespaceDatastore() (datastore.NamespaceDatastore, error) {
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &memdbNsDatastore{db: db, lastAllocatedTxn: 0}, nil
}

func (mnds *memdbNsDatastore) WriteNamespace(newConfig *pb.NamespaceDefinition) (uint64, error) {
	newVersion := mnds.lastAllocatedTxn + 1

	txn := mnds.db.Txn(true)

	foundRaw, err := txn.First("config", "id", newConfig.Name)
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

	var replacing *pb.NamespaceDefinition
	var oldVersion uint64
	if foundRaw != nil {
		found := foundRaw.(*namespace)
		replacing = found.config
		oldVersion = found.version
	}

	newConfigEntry := &namespace{
		name:    newConfig.Name,
		config:  newConfig,
		version: newVersion,
	}
	changeLogEntry := &changelog{
		id:         newVersion,
		name:       newConfig.Name,
		replaces:   replacing,
		oldVersion: oldVersion,
	}

	if err := txn.Insert("config", newConfigEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}
	if err := txn.Insert("changelog", changeLogEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

	mnds.lastAllocatedTxn++

	txn.Commit()

	return newVersion, nil
}

// ReadNamespace reads a namespace definition and version and returns it if found.
func (mnds *memdbNsDatastore) ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error) {
	txn := mnds.db.Txn(false)
	defer txn.Abort()

	foundRaw, err := txn.First("config", "id", nsName)
	if err != nil {
		return nil, 0, fmt.Errorf(errUnableToReadConfig, err)
	}

	if foundRaw == nil {
		return nil, 0, ErrNotFound
	}

	found := foundRaw.(*namespace)

	return found.config, found.version, nil
}

// Ensure that we implement the interface
var _ datastore.NamespaceDatastore = &memdbNsDatastore{}
