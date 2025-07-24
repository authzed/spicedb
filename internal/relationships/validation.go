package relationships

import (
	"context"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidateRelationshipUpdates performs validation on the given relationship updates, ensuring that
// they can be applied against the datastore.
func ValidateRelationshipUpdates(
	ctx context.Context,
	reader datastore.Reader,
	caveatTypeSet *caveattypes.TypeSet,
	updates []tuple.RelationshipUpdate,
) error {
	rels := slicez.Map(updates, func(item tuple.RelationshipUpdate) tuple.Relationship {
		return item.Relationship
	})

	// Load namespaces and caveats.
	referencedNamespaceMap, referencedCaveatMap, err := loadNamespacesAndCaveats(ctx, rels, reader)
	if err != nil {
		return err
	}

	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))

	// check if the relation is deprecated for create or touch operations
	var relsToCheck []tuple.Relationship
	for _, update := range updates {
		if update.Operation == tuple.UpdateOperationTouch || update.Operation == tuple.UpdateOperationCreate {
			relsToCheck = append(relsToCheck, update.Relationship)
		}
	}
	if err := CheckDeprecationsOnRelationships(ctx, relsToCheck, ts); err != nil {
		return err
	}

	// Validate each updates's types.
	for _, update := range updates {
		option := ValidateRelationshipForCreateOrTouch
		if update.Operation == tuple.UpdateOperationDelete {
			option = ValidateRelationshipForDeletion
		}

		if err := ValidateOneRelationship(
			referencedNamespaceMap,
			referencedCaveatMap,
			caveatTypeSet,
			update.Relationship,
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
	caveatTypeSet *caveattypes.TypeSet,
	rels ...tuple.Relationship,
) error {
	// Load namespaces and caveats.
	referencedNamespaceMap, referencedCaveatMap, err := loadNamespacesAndCaveats(ctx, rels, reader)
	if err != nil {
		return err
	}

	ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))

	// Validate if the resource, relation or subject is deprecated
	if err := CheckDeprecationsOnRelationships(ctx, rels, ts); err != nil {
		return err
	}

	// Validate each relationship's types.
	for _, rel := range rels {
		if err := ValidateOneRelationship(
			referencedNamespaceMap,
			referencedCaveatMap,
			caveatTypeSet,
			rel,
			ValidateRelationshipForCreateOrTouch,
		); err != nil {
			return err
		}
	}

	return nil
}

func loadNamespacesAndCaveats(ctx context.Context, rels []tuple.Relationship, reader datastore.Reader) (map[string]*schema.Definition, map[string]*core.CaveatDefinition, error) {
	referencedNamespaceNames := mapz.NewSet[string]()
	referencedCaveatNamesWithContext := mapz.NewSet[string]()
	for _, rel := range rels {
		referencedNamespaceNames.Insert(rel.Resource.ObjectType)
		referencedNamespaceNames.Insert(rel.Subject.ObjectType)
		if hasNonEmptyCaveatContext(rel) {
			referencedCaveatNamesWithContext.Insert(rel.OptionalCaveat.CaveatName)
		}
	}

	var referencedNamespaceMap map[string]*schema.Definition
	var referencedCaveatMap map[string]*core.CaveatDefinition

	if !referencedNamespaceNames.IsEmpty() {
		foundNamespaces, err := reader.LookupNamespacesWithNames(ctx, referencedNamespaceNames.AsSlice())
		if err != nil {
			return nil, nil, err
		}
		ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(reader))

		referencedNamespaceMap = make(map[string]*schema.Definition, len(foundNamespaces))
		for _, nsDef := range foundNamespaces {
			nts, err := schema.NewDefinition(ts, nsDef.Definition)
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
	namespaceMap map[string]*schema.Definition,
	caveatMap map[string]*core.CaveatDefinition,
	caveatTypeSet *caveattypes.TypeSet,
	rel tuple.Relationship,
	rule ValidationRelationshipRule,
) error {
	// Validate the IDs of the resource and subject.
	if err := tuple.ValidateResourceID(rel.Resource.ObjectID); err != nil {
		return err
	}

	if err := tuple.ValidateSubjectID(rel.Subject.ObjectID); err != nil {
		return err
	}

	// Validate the namespace and relation for the resource.
	resourceTS, ok := namespaceMap[rel.Resource.ObjectType]
	if !ok {
		return namespace.NewNamespaceNotFoundErr(rel.Resource.ObjectType)
	}

	if !resourceTS.HasRelation(rel.Resource.Relation) {
		return namespace.NewRelationNotFoundErr(rel.Resource.ObjectType, rel.Resource.Relation)
	}

	// Validate the namespace and relation for the subject.
	subjectTS, ok := namespaceMap[rel.Subject.ObjectType]
	if !ok {
		return namespace.NewNamespaceNotFoundErr(rel.Subject.ObjectType)
	}

	if rel.Subject.Relation != tuple.Ellipsis {
		if !subjectTS.HasRelation(rel.Subject.Relation) {
			return namespace.NewRelationNotFoundErr(rel.Subject.ObjectType, rel.Subject.Relation)
		}
	}

	// Validate that the relationship is not writing to a permission.
	if resourceTS.IsPermission(rel.Resource.Relation) {
		return NewCannotWriteToPermissionError(rel)
	}

	// Validate the subject against the allowed relation(s).
	var caveat *core.AllowedCaveat
	if rel.OptionalCaveat != nil {
		caveat = ns.AllowedCaveat(rel.OptionalCaveat.CaveatName)
	}

	var relationToCheck *core.AllowedRelation
	if rel.Subject.ObjectID == tuple.PublicWildcard {
		relationToCheck = ns.AllowedPublicNamespaceWithCaveat(rel.Subject.ObjectType, caveat)
	} else {
		relationToCheck = ns.AllowedRelationWithCaveat(
			rel.Subject.ObjectType,
			rel.Subject.Relation,
			caveat)
	}

	if rel.OptionalExpiration != nil {
		relationToCheck = ns.WithExpiration(relationToCheck)
	}

	switch {
	case rule == ValidateRelationshipForCreateOrTouch || caveat != nil:
		// For writing or when the caveat was specified, the caveat must be a direct match.
		isAllowed, err := resourceTS.HasAllowedRelation(
			rel.Resource.Relation,
			relationToCheck)
		if err != nil {
			return err
		}

		if isAllowed != schema.AllowedRelationValid {
			return NewInvalidSubjectTypeError(rel, relationToCheck, resourceTS)
		}

	case rule == ValidateRelationshipForDeletion && caveat == nil:
		// For deletion, the caveat *can* be ignored if not specified.
		if rel.Subject.ObjectID == tuple.PublicWildcard {
			isAllowed, err := resourceTS.IsAllowedPublicNamespace(rel.Resource.Relation, rel.Subject.ObjectType)
			if err != nil {
				return err
			}

			if isAllowed != schema.PublicSubjectAllowed {
				return NewInvalidSubjectTypeError(rel, relationToCheck, resourceTS)
			}
		} else {
			isAllowed, err := resourceTS.IsAllowedDirectRelation(rel.Resource.Relation, rel.Subject.ObjectType, rel.Subject.Relation)
			if err != nil {
				return err
			}

			if isAllowed != schema.DirectRelationValid {
				return NewInvalidSubjectTypeError(rel, relationToCheck, resourceTS)
			}
		}

	default:
		return spiceerrors.MustBugf("unknown validate rule")
	}

	// Validate caveat and its context, if applicable.
	if hasNonEmptyCaveatContext(rel) {
		caveat, ok := caveatMap[rel.OptionalCaveat.CaveatName]
		if !ok {
			// Should ideally never happen since the caveat is type checked above, but just in case.
			return NewCaveatNotFoundError(rel)
		}

		// Verify that the provided context information matches the types of the parameters defined.
		_, err := caveats.ConvertContextToParameters(
			caveatTypeSet,
			rel.OptionalCaveat.Context.AsMap(),
			caveat.ParameterTypes,
			caveats.ErrorForUnknownParameters,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func hasNonEmptyCaveatContext(relationship tuple.Relationship) bool {
	return relationship.OptionalCaveat != nil &&
		relationship.OptionalCaveat.CaveatName != "" &&
		relationship.OptionalCaveat.Context != nil &&
		len(relationship.OptionalCaveat.Context.GetFields()) > 0
}

func CheckDeprecationsOnRelationships(
	ctx context.Context,
	relationships []tuple.Relationship,
	ts *schema.TypeSystem,
) error {
	for _, rel := range relationships {
		if err := checkForDeprecatedRelationsAndObjects(ctx, rel, ts); err != nil {
			return err
		}
	}
	return nil
}

func checkForDeprecatedRelationsAndObjects(ctx context.Context, rel tuple.Relationship, ts *schema.TypeSystem) error {
	// Validate if the resource relation is deprecated
	relDep, ok, err := ts.GetDeprecationForRelation(ctx, rel.Resource.ObjectType, rel.Resource.Relation)
	if err != nil {
		return err
	}
	if ok {
		switch relDep.DeprecationType {
		case core.DeprecationType_DEPRECATED_TYPE_WARNING:
			log.Warn().
				Str("namespace", rel.Resource.ObjectType).
				Str("relation", rel.Resource.Relation).
				Str("comments", relDep.Comments).
				Msg("write to deprecated relation")
		case core.DeprecationType_DEPRECATED_TYPE_ERROR:
			return shared.NewDeprecationError(rel.Resource.ObjectType, rel.Resource.Relation, relDep.Comments)
		}
	}

	// Validate if the resource namespace is deprecated
	resDep, ok, err := ts.GetDeprecationForNamespace(ctx, rel.Resource.ObjectType)
	if err != nil {
		return err
	}
	if ok {
		switch resDep.DeprecationType {
		case core.DeprecationType_DEPRECATED_TYPE_WARNING:
			log.Warn().
				Str("namespace", rel.Resource.ObjectType).
				Str("comments", resDep.Comments).
				Msg("write to deprecated object")
		case core.DeprecationType_DEPRECATED_TYPE_ERROR:
			return shared.NewDeprecationError(rel.Resource.ObjectType, "", resDep.Comments)
		}
	}

	// Validate if the subject namespace is deprecated
	subDep, ok, err := ts.GetDeprecationForNamespace(ctx, rel.Subject.ObjectType)
	if err != nil {
		return err
	}
	if ok {
		switch subDep.DeprecationType {
		case core.DeprecationType_DEPRECATED_TYPE_WARNING:
			log.Warn().
				Str("namespace", rel.Subject.ObjectType).
				Str("comments", subDep.Comments).
				Msg("write to deprecated object")
		case core.DeprecationType_DEPRECATED_TYPE_ERROR:
			return shared.NewDeprecationError(rel.Subject.ObjectType, "", subDep.Comments)
		}
	}

	// check deprecation for allowed relation types
	dep, ok, err := ts.GetDeprecationForAllowedRelation(
		ctx,
		rel.Resource.ObjectType,
		rel.Resource.Relation,
		rel.Subject.ObjectType,
		rel.Subject.Relation,
		rel.Subject.ObjectID == tuple.PublicWildcard,
	)
	if err != nil {
		return err
	}

	errMsg := ""
	if rel.Subject.ObjectID == tuple.PublicWildcard {
		errMsg = tuple.PublicWildcard
	}

	if ok {
		switch dep.DeprecationType {
		case core.DeprecationType_DEPRECATED_TYPE_WARNING:
			log.Warn().
				Str("namespace", rel.Resource.ObjectType).
				Str("comments", dep.Comments).
				Msg("write to deprecated relation")
		case core.DeprecationType_DEPRECATED_TYPE_ERROR:
			return shared.NewDeprecationError(
				rel.Subject.ObjectType,
				errMsg,
				dep.Comments)
		}
	}
	return nil
}
