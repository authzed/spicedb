package memdb

import (
	"context"
	"fmt"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errUnableToWriteConfig  = "unable to write namespace config: %w"
	errUnableToReadConfig   = "unable to read namespace config: %w"
	errUnableToDeleteConfig = "unable to delete namespace config: %w"
)

func (mds *memdbDatastore) WriteNamespace(
	ctx context.Context,
	newConfig *v0.NamespaceDefinition,
) (datastore.Revision, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	newVersion, err := createNewTransaction(txn)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	foundRaw, err := txn.First(tableNamespace, indexLive, newConfig.Name, deletedTransactionID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	if foundRaw != nil {
		// Mark the old one as deleted
		var toDelete namespace = *(foundRaw.(*namespace))
		toDelete.deletedTxn = newVersion
		if err := txn.Insert(tableNamespace, &toDelete); err != nil {
			return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
		}
	}

	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	newConfigEntry := &namespace{
		name:        newConfig.Name,
		configBytes: serialized,
		createdTxn:  newVersion,
		deletedTxn:  deletedTransactionID,
	}

	time.Sleep(mds.simulatedLatency)
	if err := txn.Insert(tableNamespace, newConfigEntry); err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	txn.Commit()

	return revisionFromVersion(newVersion), nil
}

// ReadNamespace reads a namespace definition and version and returns it if found.
func (mds *memdbDatastore) ReadNamespace(
	ctx context.Context,
	nsName string,
	revision datastore.Revision,
) (*v0.NamespaceDefinition, datastore.Revision, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return nil, datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	foundIter, err := txn.Get(tableNamespace, indexID, nsName)
	if err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	foundFiltered := memdb.NewFilterIterator(foundIter, filterToLiveObjects(revision))
	foundRaw := foundFiltered.Next()
	if foundRaw == nil {
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
	}

	found := foundRaw.(*namespace)

	var loaded v0.NamespaceDefinition
	if err := proto.Unmarshal(found.configBytes, &loaded); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return &loaded, revisionFromVersion(found.createdTxn), nil
}

func (mds *memdbDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return datastore.NoRevision, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(true)
	defer txn.Abort()

	time.Sleep(mds.simulatedLatency)
	foundRaw, err := txn.First(tableNamespace, indexLive, nsName, deletedTransactionID)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}
	if foundRaw == nil {
		return datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
	}

	found := foundRaw.(*namespace)

	time.Sleep(mds.simulatedLatency)
	newChangelogID, err := createNewTransaction(txn)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Mark the namespace as deleted
	time.Sleep(mds.simulatedLatency)

	var markedDeleted namespace = *found
	markedDeleted.deletedTxn = newChangelogID
	err = txn.Insert(tableNamespace, &markedDeleted)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	// Delete the tuples in this namespace
	time.Sleep(mds.simulatedLatency)

	writeTxnID, err := mds.delete(ctx, txn, &v1.RelationshipFilter{
		ResourceType: markedDeleted.name,
	})
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	txn.Commit()

	return revisionFromVersion(writeTxnID), nil
}

func (mds *memdbDatastore) ListNamespaces(
	ctx context.Context,
	revision datastore.Revision,
) ([]*v0.NamespaceDefinition, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	var nsDefs []*v0.NamespaceDefinition

	txn := db.Txn(false)
	defer txn.Abort()

	aliveAndDead, err := txn.LowerBound(tableNamespace, indexID)
	if err != nil {
		return nil, fmt.Errorf(errUnableToReadConfig, err)
	}

	it := memdb.NewFilterIterator(aliveAndDead, filterToLiveObjects(revision))

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
