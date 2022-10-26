package shared

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// SanityCheckCaveatChanges ensures that a caveat definition being written does not break
// the types of the parameters that may already exist on relationships.
func SanityCheckCaveatChanges(
	ctx context.Context,
	rwt datastore.ReadWriteTransaction,
	caveatDef *core.CaveatDefinition,
	existingDefs map[string]*core.CaveatDefinition,
) error {
	// Ensure that the updated namespace does not break the existing tuple data.
	existing := existingDefs[caveatDef.Name]
	diff, err := caveats.DiffCaveats(existing, caveatDef)
	if err != nil {
		return err
	}

	for _, delta := range diff.Deltas() {
		switch delta.Type {
		case caveats.RemovedParameter:
			return status.Errorf(codes.InvalidArgument, "cannot remove parameter `%s` on caveat `%s`", delta.ParameterName, caveatDef.Name)

		case caveats.ParameterTypeChanged:
			return status.Errorf(codes.InvalidArgument, "cannot change the type of parameter `%s` on caveat `%s`", delta.ParameterName, caveatDef.Name)
		}
	}

	return nil
}

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
		"cannot delete object definition `%s`, as a relationship exists under it",
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
		"cannot delete object definition `%s`, as a relationship references it",
		namespaceName,
	); err != nil {
		return err
	}

	return nil
}

// SanityCheckNamespaceChanges ensures that a namespace definition being written does not result
// in breaking changes, such as relationships without associated defined schema object definitions
// and relations.
func SanityCheckNamespaceChanges(
	ctx context.Context,
	rwt datastore.ReadWriteTransaction,
	nsdef *core.NamespaceDefinition,
	existingDefs map[string]*core.NamespaceDefinition,
) (*namespace.Diff, error) {
	// Ensure that the updated namespace does not break the existing tuple data.
	existing := existingDefs[nsdef.Name]
	diff, err := namespace.DiffNamespaces(existing, nsdef)
	if err != nil {
		return nil, err
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
				"cannot delete relation `%s` in object definition `%s`, as a relationship exists under it", delta.RelationName, nsdef.Name)
			if err != nil {
				return diff, err
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
				"cannot delete relation `%s` in object definition `%s`, as a relationship references it", delta.RelationName, nsdef.Name)
			if err != nil {
				return diff, err
			}

		case namespace.RelationAllowedTypeRemoved:
			var optionalSubjectIds []string
			var relationFilter datastore.SubjectRelationFilter
			optionalCaveatName := ""

			if delta.AllowedType.GetPublicWildcard() != nil {
				optionalSubjectIds = []string{tuple.PublicWildcard}
			} else {
				relationFilter = datastore.SubjectRelationFilter{
					NonEllipsisRelation: delta.AllowedType.GetRelation(),
				}
			}

			if delta.AllowedType.GetRequiredCaveat() != nil {
				optionalCaveatName = delta.AllowedType.GetRequiredCaveat().CaveatName
			}

			qy, qyErr := rwt.QueryRelationships(
				ctx,
				datastore.RelationshipsFilter{
					ResourceType:             nsdef.Name,
					OptionalResourceRelation: delta.RelationName,
					OptionalSubjectsFilter: &datastore.SubjectsFilter{
						SubjectType:        delta.AllowedType.Namespace,
						OptionalSubjectIds: optionalSubjectIds,
						RelationFilter:     relationFilter,
					},
					OptionalCaveatName: optionalCaveatName,
				},
				options.WithLimit(options.LimitOne),
			)
			err = ErrorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot remove allowed type `%s` from relation `%s` in object definition `%s`, as a relationship exists with it",
				namespace.SourceForAllowedRelation(delta.AllowedType), delta.RelationName, nsdef.Name)
			if err != nil {
				return diff, err
			}
		}
	}
	return diff, nil
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
