package shared

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// EnsureNoRelationshipsExist ensures that no relationships exist within the namespace with the given name.
func EnsureNoRelationshipsExist(ctx context.Context, rwt datastore.ReadWriteTransaction, namespaceName string) error {
	qy, qyErr := rwt.QueryRelationships(
		ctx,
		datastore.RelationshipsFilter{ResourceType: namespaceName},
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

	qy, qyErr = rwt.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
		SubjectType: namespaceName,
	}, options.WithReverseLimit(options.LimitOne))
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
func SanityCheckExistingRelationships(
	ctx context.Context,
	rwt datastore.ReadWriteTransaction,
	nsdef *core.NamespaceDefinition,
	existingDefs map[string]*core.NamespaceDefinition,
) error {
	// Ensure that the updated namespace does not break the existing tuple data.
	existing := existingDefs[nsdef.Name]
	diff, err := namespace.DiffNamespaces(existing, nsdef)
	if err != nil {
		return err
	}

	for _, delta := range diff.Deltas() {
		switch delta.Type {
		case namespace.RemovedRelation:
			qy, qyErr := rwt.QueryRelationships(ctx, datastore.RelationshipsFilter{
				ResourceType:             nsdef.Name,
				OptionalResourceRelation: delta.RelationName,
			})

			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship exists under it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

			// Also check for right sides of tuples.
			qy, qyErr = rwt.ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
				SubjectType: nsdef.Name,
				RelationFilter: datastore.SubjectRelationFilter{
					NonEllipsisRelation: delta.RelationName,
				},
			}, options.WithReverseLimit(options.LimitOne))
			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete Relation `%s` in Object Definition `%s`, as a Relationship references it", delta.RelationName, nsdef.Name)
			if err != nil {
				return err
			}

		case namespace.RelationDirectWildcardTypeRemoved:
			qy, qyErr := rwt.ReverseQueryRelationships(
				ctx,
				datastore.SubjectsFilter{
					SubjectType:        delta.WildcardType,
					OptionalSubjectIds: []string{tuple.PublicWildcard},
				},
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
			qy, qyErr := rwt.ReverseQueryRelationships(
				ctx,
				datastore.SubjectsFilter{
					SubjectType: delta.DirectType.Namespace,
					RelationFilter: datastore.SubjectRelationFilter{
						NonEllipsisRelation: delta.RelationName,
					},
				},
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
func ErrorIfTupleIteratorReturnsTuples(ctx context.Context, qy datastore.RelationshipIterator, qyErr error, message string, args ...interface{}) error {
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
