package shared

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
)

// EnsureNoRelationshipsExist ensures that no relationships exist within the namespace with the given name.
func EnsureNoRelationshipsExist(ctx context.Context, ds datastore.Datastore, namespaceName string) error {
	syncRevision, err := ds.SyncRevision(ctx)
	if err != nil {
		return err
	}

	if err := errorIfTupleIteratorReturnsTuples(
		ctx,
		ds.QueryTuples(datastore.TupleQueryResourceFilter{ResourceType: namespaceName}, syncRevision),
		"cannot delete Object Definition `%s`, as a Relationship exists under it",
		namespaceName,
	); err != nil {
		return err
	}

	if err := errorIfTupleIteratorReturnsTuples(
		ctx,
		ds.ReverseQueryTuplesFromSubjectNamespace(namespaceName, syncRevision),
		"cannot delete Object Definition `%s`, as a Relationship references it",
		namespaceName,
	); err != nil {
		return err
	}

	return nil
}

// SanityCheckExistingRelationships ensures that a namespace definition being written does not result
// in relationships without associated defined schema object definitions and relations.
func SanityCheckExistingRelationships(ctx context.Context, ds datastore.Datastore, nsdef *v0.NamespaceDefinition) error {
	// Ensure that the updated namespace does not break the existing tuple data.
	//
	// NOTE: We use the datastore here to read the namespace, rather than the namespace manager,
	// to ensure there is no caching being used.
	existing, _, err := ds.ReadNamespace(ctx, nsdef.Name)
	if err != nil && !errors.As(err, &datastore.ErrNamespaceNotFound{}) {
		return err
	}

	diff, err := namespace.DiffNamespaces(existing, nsdef)
	if err != nil {
		return err
	}

	syncRevision, err := ds.SyncRevision(ctx)
	if err != nil {
		return err
	}

	for _, delta := range diff.Deltas() {
		switch delta.Type {
		case namespace.RemovedRelation:
			err = errorIfTupleIteratorReturnsTuples(
				ctx,
				ds.QueryTuples(datastore.TupleQueryResourceFilter{
					ResourceType:             nsdef.Name,
					OptionalResourceRelation: delta.RelationName,
				}, syncRevision),
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship exists under it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

			// Also check for right sides of tuples.
			err = errorIfTupleIteratorReturnsTuples(
				ctx,
				ds.ReverseQueryTuplesFromSubjectRelation(nsdef.Name, delta.RelationName, syncRevision),
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship references it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

		case namespace.RelationDirectTypeRemoved:
			err = errorIfTupleIteratorReturnsTuples(
				ctx,
				ds.ReverseQueryTuplesFromSubjectRelation(delta.DirectType.Namespace, delta.DirectType.Relation, syncRevision).
					WithObjectRelation(nsdef.Name, delta.RelationName),
				"cannot remove allowed direct Relation `%s#%s` from Relation `%s` in Object Definition `%s`, as a Relationship exists with it",
				delta.DirectType.Namespace, delta.DirectType.Relation, delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func errorIfTupleIteratorReturnsTuples(ctx context.Context, query datastore.CommonTupleQuery, message string, args ...interface{}) error {
	qy, err := query.Limit(1).Execute(ctx)
	if err != nil {
		return err
	}
	defer qy.Close()

	rt := qy.Next()
	if rt != nil {
		if qy.Err() != nil {
			return qy.Err()
		}

		return status.Errorf(codes.InvalidArgument, message, args...)
	}
	return nil
}
