package shared

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/namespace"
)

// EnsureNoRelationshipsExist ensures that no relationships exist within the namespace with the given name.
func EnsureNoRelationshipsExist(ctx context.Context, ds datastore.Datastore, namespaceName string) error {
	headRevision, err := ds.HeadRevision(ctx)
	if err != nil {
		return err
	}

	qy, qyErr := ds.QueryTuples(
		ctx,
		&v1.RelationshipFilter{ResourceType: namespaceName},
		headRevision,
		options.WithLimit(options.LimitOne),
	)
	if err := ErrorIfTupleIteratorReturnsTuples(
		ctx,
		qy,
		qyErr,
		"cannot delete Object Definition `%s`, as a Relationship exists under it",
		namespaceName,
	); err != nil {
		return err
	}

	qy, qyErr = ds.ReverseQueryTuplesFromSubjectNamespace(namespaceName, headRevision).Limit(1).Execute(ctx)
	if err := ErrorIfTupleIteratorReturnsTuples(
		ctx,
		qy,
		qyErr,
		"cannot delete Object Definition `%s`, as a Relationship references it",
		namespaceName,
	); err != nil {
		return err
	}

	return nil
}

// SanityCheckExistingRelationships ensures that a namespace definition being written does not result
// in relationships without associated defined schema object definitions and relations.
func SanityCheckExistingRelationships(ctx context.Context, ds datastore.Datastore, nsdef *v0.NamespaceDefinition, revision decimal.Decimal) error {
	// Ensure that the updated namespace does not break the existing tuple data.
	//
	// NOTE: We use the datastore here to read the namespace, rather than the namespace manager,
	// to ensure there is no caching being used.
	existing, _, err := ds.ReadNamespace(ctx, nsdef.Name, revision)
	if err != nil && !errors.As(err, &datastore.ErrNamespaceNotFound{}) {
		return err
	}

	diff, err := namespace.DiffNamespaces(existing, nsdef)
	if err != nil {
		return err
	}

	headRevision, err := ds.HeadRevision(ctx)
	if err != nil {
		return err
	}

	for _, delta := range diff.Deltas() {
		switch delta.Type {
		case namespace.RemovedRelation:
			qy, qyErr := ds.QueryTuples(ctx, &v1.RelationshipFilter{
				ResourceType:     nsdef.Name,
				OptionalRelation: delta.RelationName,
			}, headRevision)

			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship exists under it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

			// Also check for right sides of tuples.
			qy, qyErr = ds.ReverseQueryTuplesFromSubjectRelation(nsdef.Name, delta.RelationName, headRevision).Limit(1).Execute(ctx)
			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship references it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

		case namespace.RelationDirectTypeRemoved:
			qy, qyErr := ds.ReverseQueryTuplesFromSubjectRelation(delta.DirectType.Namespace, delta.DirectType.Relation, headRevision).
				WithObjectRelation(nsdef.Name, delta.RelationName).Limit(1).Execute(ctx)

			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot remove allowed direct Relation `%s#%s` from Relation `%s` in Object Definition `%s`, as a Relationship exists with it",
				delta.DirectType.Namespace, delta.DirectType.Relation, delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrorIfTupleIteratorReturnsTuples takes a tuple iterator and any error that was generated
// when the original iterator was created, and returns an error if iterator contains any tuples.
func ErrorIfTupleIteratorReturnsTuples(ctx context.Context, qy datastore.TupleIterator, qyErr error, message string, args ...interface{}) error {
	if qyErr != nil {
		return qyErr
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
