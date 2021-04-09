package memdb

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errUnableToInstantiate  = "unable to instantiate datastore: %w"
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

func (mds *memdbDatastore) WriteNamespace(ctx context.Context, newConfig *pb.NamespaceDefinition) (uint64, error) {
	txn := mds.db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	newVersion, err := nextChangelogID(txn)
	if err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

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

	time.Sleep(mds.simulatedLatency)
	if err := txn.Insert(tableNamespaceConfig, newConfigEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

	time.Sleep(mds.simulatedLatency)
	if err := txn.Insert(tableNamespaceChangelog, changeLogEntry); err != nil {
		return 0, fmt.Errorf(errUnableToWriteConfig, err)
	}

	txn.Commit()

	return newVersion, nil
}

// ReadNamespace reads a namespace definition and version and returns it if found.
func (mds *memdbDatastore) ReadNamespace(ctx context.Context, nsName string) (*pb.NamespaceDefinition, uint64, error) {
	txn := mds.db.Txn(false)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
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

func (mds *memdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (uint64, error) {
	txn := mds.db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	foundRaw, err := txn.First(tableNamespaceConfig, indexID, nsName)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	if foundRaw == nil {
		return 0, datastore.ErrNamespaceNotFound
	}

	found := foundRaw.(*namespace)

	time.Sleep(mds.simulatedLatency)
	newChangelogID, err := nextChangelogID(txn)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	changeLogEntry := &changelog{
		id:         newChangelogID,
		name:       nsName,
		replaces:   found.config,
		oldVersion: found.version,
	}

	// Delete the namespace config
	time.Sleep(mds.simulatedLatency)
	err = txn.Delete(tableNamespaceConfig, found)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Write the changelog that we delete the namespace
	time.Sleep(mds.simulatedLatency)
	err = txn.Insert(tableNamespaceChangelog, changeLogEntry)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Delete the tuples in this namespace
	time.Sleep(mds.simulatedLatency)
	_, err = txn.DeleteAll(tableTuple, indexNamespace, nsName)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	txn.Commit()

	return found.version, nil
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
