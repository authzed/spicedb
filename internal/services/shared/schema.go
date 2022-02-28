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
	"github.com/authzed/spicedb/pkg/tuple"
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

	qy, qyErr = ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
		SubjectType: namespaceName,
	}, headRevision, options.WithReverseLimit(options.LimitOne))
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
			qy, qyErr = ds.ReverseQueryTuples(ctx, &v1.SubjectFilter{
				SubjectType: nsdef.Name,
				OptionalRelation: &v1.SubjectFilter_RelationFilter{
					Relation: delta.RelationName,
				},
			}, headRevision, options.WithReverseLimit(options.LimitOne))
			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship references it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

		case namespace.RelationDirectWildcardTypeRemoved:
			qy, qyErr := ds.ReverseQueryTuples(
				ctx,
				&v1.SubjectFilter{
					SubjectType:       delta.WildcardType,
					OptionalSubjectId: tuple.PublicWildcard,
				},
				headRevision,
				options.WithResRelation(&options.ResourceRelation{
					Namespace: nsdef.Name,
					Relation:  delta.RelationName,
				}),
				options.WithReverseLimit(options.LimitOne),
			)
			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot remove allowed wildcard type `%s:*` from Relation `%s` in Object Definition `%s`, as a Relationship exists with it",
				delta.WildcardType, delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

		case namespace.RelationDirectTypeRemoved:
			qy, qyErr := ds.ReverseQueryTuples(
				ctx,
				&v1.SubjectFilter{
					SubjectType: delta.DirectType.Namespace,
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: delta.DirectType.Relation,
					},
				},
				headRevision,
				options.WithResRelation(&options.ResourceRelation{
					Namespace: nsdef.Name,
					Relation:  delta.RelationName,
				}),
				options.WithReverseLimit(options.LimitOne),
			)
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

	if rt := qy.Next(); rt != nil {
		if qy.Err() != nil {
			return qy.Err()
		}

		return status.Errorf(codes.InvalidArgument, message, args...)
	}
	return nil
}
