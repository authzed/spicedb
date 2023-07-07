package relationships

import (
	"context"

	"github.com/samber/lo"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidateRelationshipUpdates performs validation on the given relationship updates, ensuring that
// they can be applied against the datastore.
func ValidateRelationshipUpdates(
	ctx context.Context,
	reader datastore.Reader,
	updates []*core.RelationTupleUpdate,
) error {
	rels := lo.Map(updates, func(item *core.RelationTupleUpdate, _ int) *core.RelationTuple {
		return item.Tuple
	})

	// Load namespaces and caveats.
	referencedNamespaceMap, referencedCaveatMap, err := loadNamespacesAndCaveats(ctx, rels, reader)
	if err != nil {
		return err
	}

	// Validate each updates's types.
	for _, update := range updates {
		option := ValidateRelationshipForCreateOrTouch
		if update.Operation == core.RelationTupleUpdate_DELETE {
			option = ValidateRelationshipForDeletion
		}

		if err := ValidateOneRelationship(
			referencedNamespaceMap,
			referencedCaveatMap,
			update.Tuple,
			option,
		); err != nil {
			return err
		}
	}

	return nil
}

// ValidateRelationshipsForCreateOrTouch performs validation on the given relationships to be written, ensuring that
// they can be applied against the datastore.
//
// NOTE: This method *cannot* be used for relationships that will be deleted.
func ValidateRelationshipsForCreateOrTouch(
	ctx context.Context,
	reader datastore.Reader,
	rels []*core.RelationTuple,
) error {
	// Load namespaces and caveats.
	referencedNamespaceMap, referencedCaveatMap, err := loadNamespacesAndCaveats(ctx, rels, reader)
	if err != nil {
		return err
	}

	// Validate each relationship's types.
	for _, rel := range rels {
		if err := ValidateOneRelationship(
			referencedNamespaceMap,
			referencedCaveatMap,
			rel,
			ValidateRelationshipForCreateOrTouch,
		); err != nil {
			return err
		}
	}

	return nil
}

func loadNamespacesAndCaveats(ctx context.Context, rels []*core.RelationTuple, reader datastore.Reader) (map[string]*namespace.TypeSystem, map[string]*core.CaveatDefinition, error) {
	referencedNamespaceNames := mapz.NewSet[string]()
	referencedCaveatNamesWithContext := mapz.NewSet[string]()
	for _, rel := range rels {
		referencedNamespaceNames.Add(rel.ResourceAndRelation.Namespace)
		referencedNamespaceNames.Add(rel.Subject.Namespace)
		if hasNonEmptyCaveatContext(rel) {
			referencedCaveatNamesWithContext.Add(rel.Caveat.CaveatName)
		}
	}

	var referencedNamespaceMap map[string]*namespace.TypeSystem
	var referencedCaveatMap map[string]*core.CaveatDefinition

	if !referencedNamespaceNames.IsEmpty() {
		foundNamespaces, err := reader.LookupNamespacesWithNames(ctx, referencedNamespaceNames.AsSlice())
		if err != nil {
			return nil, nil, err
		}

		referencedNamespaceMap = make(map[string]*namespace.TypeSystem, len(foundNamespaces))
		for _, nsDef := range foundNamespaces {
			nts, err := namespace.NewNamespaceTypeSystem(nsDef.Definition, namespace.ResolverForDatastoreReader(reader))
			if err != nil {
				return nil, nil, err
			}

			referencedNamespaceMap[nsDef.Definition.Name] = nts
		}
	}

	if !referencedCaveatNamesWithContext.IsEmpty() {
		foundCaveats, err := reader.LookupCaveatsWithNames(ctx, referencedCaveatNamesWithContext.AsSlice())
		if err != nil {
			return nil, nil, err
		}

		referencedCaveatMap = make(map[string]*core.CaveatDefinition, len(foundCaveats))
		for _, caveatDef := range foundCaveats {
			referencedCaveatMap[caveatDef.Definition.Name] = caveatDef.Definition
		}
	}
	return referencedNamespaceMap, referencedCaveatMap, nil
}

// ValidationRelationshipRule is the rule to use for the validation.
type ValidationRelationshipRule int

const (
	// ValidateRelationshipForCreateOrTouch indicates that the validation should occur for a CREATE or TOUCH operation.
	ValidateRelationshipForCreateOrTouch ValidationRelationshipRule = 0

	// ValidateRelationshipForDeletion indicates that the validation should occur for a DELETE operation.
	ValidateRelationshipForDeletion ValidationRelationshipRule = 1
)

// ValidateOneRelationship validates a single relationship for CREATE/TOUCH or DELETE.
func ValidateOneRelationship(
	namespaceMap map[string]*namespace.TypeSystem,
	caveatMap map[string]*core.CaveatDefinition,
	rel *core.RelationTuple,
	rule ValidationRelationshipRule,
) error {
	// Validate the IDs of the resource and subject.
	if err := tuple.ValidateResourceID(rel.ResourceAndRelation.ObjectId); err != nil {
		return err
	}

	if err := tuple.ValidateSubjectID(rel.Subject.ObjectId); err != nil {
		return err
	}

	// Validate the namespace and relation for the resource.
	resourceTS, ok := namespaceMap[rel.ResourceAndRelation.Namespace]
	if !ok {
		return namespace.NewNamespaceNotFoundErr(rel.ResourceAndRelation.Namespace)
	}

	if !resourceTS.HasRelation(rel.ResourceAndRelation.Relation) {
		return namespace.NewRelationNotFoundErr(rel.ResourceAndRelation.Namespace, rel.ResourceAndRelation.Relation)
	}

	// Validate the namespace and relation for the subject.
	subjectTS, ok := namespaceMap[rel.Subject.Namespace]
	if !ok {
		return namespace.NewNamespaceNotFoundErr(rel.Subject.Namespace)
	}

	if rel.Subject.Relation != tuple.Ellipsis {
		if !subjectTS.HasRelation(rel.Subject.Relation) {
			return namespace.NewRelationNotFoundErr(rel.Subject.Namespace, rel.Subject.Relation)
		}
	}

	// Validate that the relationship is not writing to a permission.
	if resourceTS.IsPermission(rel.ResourceAndRelation.Relation) {
		return NewCannotWriteToPermissionError(rel)
	}

	// Validate the subject against the allowed relation(s).
	var caveat *core.AllowedCaveat
	if rel.Caveat != nil {
		caveat = ns.AllowedCaveat(rel.Caveat.CaveatName)
	}

	var relationToCheck *core.AllowedRelation
	if rel.Subject.ObjectId == tuple.PublicWildcard {
		relationToCheck = ns.AllowedPublicNamespaceWithCaveat(rel.Subject.Namespace, caveat)
	} else {
		relationToCheck = ns.AllowedRelationWithCaveat(
			rel.Subject.Namespace,
			rel.Subject.Relation,
			caveat)
	}

	switch {
	case rule == ValidateRelationshipForCreateOrTouch || caveat != nil:
		// For writing or when the caveat was specified, the caveat must be a direct match.
		isAllowed, err := resourceTS.HasAllowedRelation(
			rel.ResourceAndRelation.Relation,
			relationToCheck)
		if err != nil {
			return err
		}

		if isAllowed != namespace.AllowedRelationValid {
			return NewInvalidSubjectTypeError(rel, relationToCheck)
		}

	case rule == ValidateRelationshipForDeletion && caveat == nil:
		// For deletion, the caveat *can* be ignored if not specified.
		if rel.Subject.ObjectId == tuple.PublicWildcard {
			isAllowed, err := resourceTS.IsAllowedPublicNamespace(rel.ResourceAndRelation.Relation, rel.Subject.Namespace)
			if err != nil {
				return err
			}

			if isAllowed != namespace.PublicSubjectAllowed {
				return NewInvalidSubjectTypeError(rel, relationToCheck)
			}
		} else {
			isAllowed, err := resourceTS.IsAllowedDirectRelation(rel.ResourceAndRelation.Relation, rel.Subject.Namespace, rel.Subject.Relation)
			if err != nil {
				return err
			}

			if isAllowed != namespace.DirectRelationValid {
				return NewInvalidSubjectTypeError(rel, relationToCheck)
			}
		}

	default:
		return spiceerrors.MustBugf("unknown validate rule")
	}

	// Validate caveat and its context, if applicable.
	if hasNonEmptyCaveatContext(rel) {
		caveat, ok := caveatMap[rel.Caveat.CaveatName]
		if !ok {
			// Should ideally never happen since the caveat is type checked above, but just in case.
			return NewCaveatNotFoundError(rel)
		}

		// Verify that the provided context information matches the types of the parameters defined.
		_, err := caveats.ConvertContextToParameters(
			rel.Caveat.Context.AsMap(),
			caveat.ParameterTypes,
			caveats.ErrorForUnknownParameters,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func hasNonEmptyCaveatContext(update *core.RelationTuple) bool {
	return update.Caveat != nil &&
		update.Caveat.CaveatName != "" &&
		update.Caveat.Context != nil &&
		len(update.Caveat.Context.GetFields()) > 0
}
