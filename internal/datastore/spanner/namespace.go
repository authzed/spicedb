package spanner

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (sd spannerDatastore) WriteNamespace(ctx context.Context, newConfig *core.NamespaceDefinition) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "WriteNamespace")
	defer span.End()
	serialized, err := proto.Marshal(newConfig)
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	ts, err := sd.client.ReadWriteTransaction(ctx, func(c context.Context, rwt *spanner.ReadWriteTransaction) error {
		return rwt.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdate(
				tableNamespace,
				[]string{colNamespaceName, colNamespaceConfig, colTimestamp},
				[]interface{}{newConfig.Name, serialized, spanner.CommitTimestamp},
			),
		})
	})
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteConfig, err)
	}

	return revisionFromTimestamp(ts), nil
}

func (sd spannerDatastore) ReadNamespace(ctx context.Context, nsName string, revision datastore.Revision) (*core.NamespaceDefinition, datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace")
	defer span.End()
	ts := timestampFromRevision(revision)

	nsKey := spanner.Key{nsName}
	row, err := sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(ts)).ReadRow(
		ctx,
		tableNamespace,
		nsKey,
		[]string{colNamespaceConfig, colNamespaceTS},
	)
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
		}
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	var serialized []byte
	var updated time.Time
	if err := row.Columns(&serialized, &updated); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	ns := &core.NamespaceDefinition{}
	if err := proto.Unmarshal(serialized, ns); err != nil {
		return nil, datastore.NoRevision, fmt.Errorf(errUnableToReadConfig, err)
	}

	return ns, revisionFromTimestamp(updated), nil
}

func (sd spannerDatastore) DeleteNamespace(ctx context.Context, nsName string) (datastore.Revision, error) {
	ctx, span := tracer.Start(ctx, "DeleteNamespace")
	defer span.End()
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		if err := deleteWithFilter(ctx, rwt, &v1.RelationshipFilter{
			ResourceType: nsName,
		}); err != nil {
			return err
		}

		return rwt.BufferWrite([]*spanner.Mutation{
			spanner.Delete(tableNamespace, spanner.KeySetFromKeys(spanner.Key{nsName})),
		})
	})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
		}
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return revisionFromTimestamp(ts), nil
}

func (sd spannerDatastore) ListNamespaces(ctx context.Context, revision datastore.Revision) ([]*core.NamespaceDefinition, error) {
	ctx, span := tracer.Start(ctx, "ListNamespaces")
	defer span.End()
	ts := timestampFromRevision(revision)

	iter := sd.client.Single().WithTimestampBound(spanner.ReadTimestamp(ts)).Read(
		ctx,
		tableNamespace,
		spanner.AllKeys(),
		[]string{colNamespaceConfig},
	)

	allNamespaces, err := readAllNamespaces(iter)
	if err != nil {
		return nil, fmt.Errorf(errUnableToListNamespaces, err)
	}

	return allNamespaces, nil
}

func readAllNamespaces(iter *spanner.RowIterator) ([]*core.NamespaceDefinition, error) {
	var allNamespaces []*core.NamespaceDefinition
	if err := iter.Do(func(row *spanner.Row) error {
		var serialized []byte
		if err := row.Columns(&serialized); err != nil {
			return err
		}

		ns := &core.NamespaceDefinition{}
		if err := proto.Unmarshal(serialized, ns); err != nil {
			return err
		}

		allNamespaces = append(allNamespaces, ns)

		return nil
	}); err != nil {
		return nil, err
	}

	return allNamespaces, nil
}
