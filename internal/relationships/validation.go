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

// ValidateRelationshipUpdates performs validation on the given relationship updates, ensuring that
// they can be applied against the datastore.
func ValidateRelationshipUpdates(
	ctx context.Context,
	reader datastore.Reader,
	updates []*core.RelationTupleUpdate,
) error {
	referencedNamespaceNames := util.NewSet[string]()
	referencedCaveatNamesWithContext := util.NewSet[string]()
	for _, update := range updates {
		referencedNamespaceNames.Add(update.Tuple.ResourceAndRelation.Namespace)
		referencedNamespaceNames.Add(update.Tuple.Subject.Namespace)
		if hasNonEmptyCaveatContext(update) {
			referencedCaveatNamesWithContext.Add(update.Tuple.Caveat.CaveatName)
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

	// Validate each update's types.
	for _, update := range updates {
		// Validate the IDs of the resource and subject.
		if err := tuple.ValidateResourceID(update.Tuple.ResourceAndRelation.ObjectId); err != nil {
			return err
		}

		if err := tuple.ValidateSubjectID(update.Tuple.Subject.ObjectId); err != nil {
			return err
		}

		// Validate the namespace and relation for the resource.
		resourceTS, ok := referencedNamespaceMap[update.Tuple.ResourceAndRelation.Namespace]
		if !ok {
			return namespace.NewNamespaceNotFoundErr(update.Tuple.ResourceAndRelation.Namespace)
		}

		if !resourceTS.HasRelation(update.Tuple.ResourceAndRelation.Relation) {
			return namespace.NewRelationNotFoundErr(update.Tuple.ResourceAndRelation.Namespace, update.Tuple.ResourceAndRelation.Relation)
		}

		// Validate the namespace and relation for the subject.
		subjectTS, ok := referencedNamespaceMap[update.Tuple.Subject.Namespace]
		if !ok {
			return namespace.NewNamespaceNotFoundErr(update.Tuple.Subject.Namespace)
		}

		if update.Tuple.Subject.Relation != tuple.Ellipsis {
			if !subjectTS.HasRelation(update.Tuple.Subject.Relation) {
				return namespace.NewRelationNotFoundErr(update.Tuple.Subject.Namespace, update.Tuple.Subject.Relation)
			}
		}

		// Validate that the relationship is not writing to a permission.
		if resourceTS.IsPermission(update.Tuple.ResourceAndRelation.Relation) {
			return NewCannotWriteToPermissionError(update)
		}

		// Validate the subject against the allowed relation(s).
		var relationToCheck *core.AllowedRelation
		var caveat *core.AllowedCaveat

		if update.Tuple.Caveat != nil {
			caveat = ns.AllowedCaveat(update.Tuple.Caveat.CaveatName)
		}

		if update.Tuple.Subject.ObjectId == tuple.PublicWildcard {
			relationToCheck = ns.AllowedPublicNamespaceWithCaveat(update.Tuple.Subject.Namespace, caveat)
		} else {
			relationToCheck = ns.AllowedRelationWithCaveat(
				update.Tuple.Subject.Namespace,
				update.Tuple.Subject.Relation,
				caveat)
		}

		isAllowed, err := resourceTS.HasAllowedRelation(
			update.Tuple.ResourceAndRelation.Relation,
			relationToCheck,
		)
		if err != nil {
			return err
		}

		if isAllowed != namespace.AllowedRelationValid {
			return NewInvalidSubjectTypeError(update, relationToCheck)
		}

		// Validate caveat and its context, if applicable.
		if hasNonEmptyCaveatContext(update) {
			caveat, ok := referencedCaveatMap[update.Tuple.Caveat.CaveatName]
			if !ok {
				// Should ideally never happen since the caveat is type checked above, but just in case.
				return NewCaveatNotFoundError(update)
			}

			// Verify that the provided context information matches the types of the parameters defined.
			_, err := caveats.ConvertContextToParameters(
				update.Tuple.Caveat.Context.AsMap(),
				caveat.ParameterTypes,
				caveats.ErrorForUnknownParameters,
			)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func hasNonEmptyCaveatContext(update *core.RelationTupleUpdate) bool {
	return update.Tuple.Caveat != nil &&
		update.Tuple.Caveat.CaveatName != "" &&
		update.Tuple.Caveat.Context != nil &&
		len(update.Tuple.Caveat.Context.GetFields()) > 0
}
