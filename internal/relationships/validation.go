package relationships

import (
	"context"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/util"
)

// ValidateRelationships performs validation on the given relationships, ensuring that
// they can be applied against the datastore.
func ValidateRelationships(
	ctx context.Context,
	reader datastore.Reader,
	rels []*core.RelationTuple,
) error {
	referencedNamespaceNames := util.NewSet[string]()
	referencedCaveatNamesWithContext := util.NewSet[string]()
	for _, rel := range rels {
		referencedNamespaceNames.Add(rel.ResourceAndRelation.Namespace)
		referencedNamespaceNames.Add(rel.Subject.Namespace)
		if hasNonEmptyCaveatContext(rel) {
			referencedCaveatNamesWithContext.Add(rel.Caveat.CaveatName)
		}
	}

	var referencedNamespaceMap map[string]*namespace.TypeSystem
	var referencedCaveatMap map[string]*core.CaveatDefinition

	// Load namespaces.
	if !referencedNamespaceNames.IsEmpty() {
		foundNamespaces, err := reader.LookupNamespacesWithNames(ctx, referencedNamespaceNames.AsSlice())
		if err != nil {
			return err
		}

		referencedNamespaceMap = make(map[string]*namespace.TypeSystem, len(foundNamespaces))
		for _, nsDef := range foundNamespaces {
			nts, err := namespace.NewNamespaceTypeSystem(nsDef.Definition, namespace.ResolverForDatastoreReader(reader))
			if err != nil {
				return err
			}

			referencedNamespaceMap[nsDef.Definition.Name] = nts
		}
	}

	// Load caveats, if any.
	if !referencedCaveatNamesWithContext.IsEmpty() {
		foundCaveats, err := reader.LookupCaveatsWithNames(ctx, referencedCaveatNamesWithContext.AsSlice())
		if err != nil {
			return err
		}

		referencedCaveatMap = make(map[string]*core.CaveatDefinition, len(foundCaveats))
		for _, caveatDef := range foundCaveats {
			referencedCaveatMap[caveatDef.Definition.Name] = caveatDef.Definition
		}
	}

	// Validate each relationship's types.
	for _, rel := range rels {
		if err := ValidateOneRelationship(
			referencedNamespaceMap,
			referencedCaveatMap,
			rel,
		); err != nil {
			return err
		}
	}

	return nil
}

func ValidateOneRelationship(
	namespaceMap map[string]*namespace.TypeSystem,
	caveatMap map[string]*core.CaveatDefinition,
	rel *core.RelationTuple,
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
	var relationToCheck *core.AllowedRelation
	var caveat *core.AllowedCaveat

	if rel.Caveat != nil {
		caveat = ns.AllowedCaveat(rel.Caveat.CaveatName)
	}

	if rel.Subject.ObjectId == tuple.PublicWildcard {
		relationToCheck = ns.AllowedPublicNamespaceWithCaveat(rel.Subject.Namespace, caveat)
	} else {
		relationToCheck = ns.AllowedRelationWithCaveat(
			rel.Subject.Namespace,
			rel.Subject.Relation,
			caveat)
	}

	isAllowed, err := resourceTS.HasAllowedRelation(
		rel.ResourceAndRelation.Relation,
		relationToCheck,
	)
	if err != nil {
		return err
	}

	if isAllowed != namespace.AllowedRelationValid {
		return NewInvalidSubjectTypeError(rel, relationToCheck)
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
