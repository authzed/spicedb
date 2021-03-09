package memdb

import (
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToInstantiate = "unable to instantiate datastore: %w"
	errUnableToWriteConfig = "unable to write namespace config: %w"
	errUnableToReadConfig  = "unable to read namespace config: %w"
)

func (mds *memdbDatastore) WriteNamespace(newConfig *pb.NamespaceDefinition) (uint64, error) {
	txn := mds.db.Txn(true)

	newVersion, err := nextChangelogID(txn)

	foundRaw, err := txn.First(tableNamespaceConfig, indexID, newConfig.Name)
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

	if err := txn.Insert(tableNamespaceConfig, newConfigEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}
	if err := txn.Insert(tableNamespaceChangelog, changeLogEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

	txn.Commit()

	return newVersion, nil
}

// ReadNamespace reads a namespace definition and version and returns it if found.
func (mds *memdbDatastore) ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error) {
	txn := mds.db.Txn(false)
	defer txn.Abort()

	foundRaw, err := txn.First(tableNamespaceConfig, indexID, nsName)
	if err != nil {
		return nil, 0, fmt.Errorf(errUnableToReadConfig, err)
	}

	if foundRaw == nil {
		return nil, 0, datastore.ErrNamespaceNotFound
	}

	found := foundRaw.(*namespace)

	return found.config, found.version, nil
}

func nextChangelogID(txn *memdb.Txn) (uint64, error) {
	lastChangeRaw, err := txn.Last(tableNamespaceChangelog, indexID)
	if err != nil {
		return 0, err
	}

	if lastChangeRaw == nil {
		return 1, nil
	}

	return lastChangeRaw.(*changelog).id + 1, nil
}
