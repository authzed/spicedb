package memdb

import (
	"context"
	"fmt"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/hashicorp/go-memdb"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

func (mds *memdbDatastore) WriteNamespace(ctx context.Context, newConfig *v0.NamespaceDefinition) (datastore.Revision, error) {
	if mds.db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := mds.db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	newVersion, err := nextChangelogID(txn)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	foundRaw, err := txn.First(tableNamespaceConfig, indexID, newConfig.Name)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	var replacing []byte
	var oldVersion uint64
	if foundRaw != nil {
		found := foundRaw.(*namespace)
		replacing = found.configBytes
		oldVersion = found.version
	}

	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	newConfigEntry := &namespace{
		name:        newConfig.Name,
		configBytes: serialized,
		version:     newVersion,
	}
	changeLogEntry := &changelog{
		id:         newVersion,
		name:       newConfig.Name,
		replaces:   replacing,
		oldVersion: oldVersion,
	}

	time.Sleep(mds.simulatedLatency)
	if err := txn.Insert(tableNamespaceConfig, newConfigEntry); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	time.Sleep(mds.simulatedLatency)
	if err := txn.Insert(tableNamespaceChangelog, changeLogEntry); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	txn.Commit()

	return revisionFromVersion(newVersion), nil
}

// ReadNamespace reads a namespace definition and version and returns it if found.
func (mds *memdbDatastore) ReadNamespace(ctx context.Context, nsName string) (*v0.NamespaceDefinition, datastore.Revision, error) {
	if mds.db == nil {
		return nil, datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := mds.db.Txn(false)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	foundRaw, err := txn.First(tableNamespaceConfig, indexID, nsName)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	if foundRaw == nil {
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
	}

	found := foundRaw.(*namespace)

	var loaded v0.NamespaceDefinition
	if err := proto.Unmarshal(found.configBytes, &loaded); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return &loaded, revisionFromVersion(found.version), nil
}

func (mds *memdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	if mds.db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := mds.db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	foundRaw, err := txn.First(tableNamespaceConfig, indexID, nsName)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	if foundRaw == nil {
		return datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
	}

	found := foundRaw.(*namespace)

	time.Sleep(mds.simulatedLatency)
	newChangelogID, err := nextChangelogID(txn)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	changeLogEntry := &changelog{
		id:         newChangelogID,
		name:       nsName,
		replaces:   found.configBytes,
		oldVersion: found.version,
	}

	// Delete the namespace config
	time.Sleep(mds.simulatedLatency)
	err = txn.Delete(tableNamespaceConfig, found)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Write the changelog that we delete the namespace
	time.Sleep(mds.simulatedLatency)
	err = txn.Insert(tableNamespaceChangelog, changeLogEntry)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Delete the tuples in this namespace
	time.Sleep(mds.simulatedLatency)
	_, err = txn.DeleteAll(tableTuple, indexNamespace, nsName)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	txn.Commit()

	return revisionFromVersion(found.version), nil
}

func (mds *memdbDatastore) ListNamespaces(ctx context.Context) ([]*v0.NamespaceDefinition, error) {
	if mds.db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	var nsDefs []*v0.NamespaceDefinition

	txn := mds.db.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(tableNamespaceConfig, indexID)
	if err != nil {
		return nsDefs, err
	}

	for {
		foundRaw := it.Next()
		if foundRaw == nil {
			break
		}

		found := foundRaw.(*namespace)
		var loaded v0.NamespaceDefinition
		if err := proto.Unmarshal(found.configBytes, &loaded); err != nil {
			return nil, fmt.Errorf(errUnableToReadConfig, err)
		}

		nsDefs = append(nsDefs, &loaded)
	}

	return nsDefs, nil
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
