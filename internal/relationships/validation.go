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
	// Load caveats, if any.
	var referencedCaveatMap map[string]*core.CaveatDefinition
	referencedCaveatNamesWithContext := util.NewSet[string]()

	for _, update := range updates {
		if hasNonEmptyCaveatContext(update) {
			referencedCaveatNamesWithContext.Add(update.Tuple.Caveat.CaveatName)
		}
	}

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

	// TODO(jschorr): look into loading the type system once per type, rather than once per relationship
	// Check each update.
	for _, update := range updates {
		// Validate the IDs of the resource and subject.
		if err := tuple.ValidateResourceID(update.Tuple.ResourceAndRelation.ObjectId); err != nil {
			return err
		}

		if err := tuple.ValidateSubjectID(update.Tuple.Subject.ObjectId); err != nil {
			return err
		}

		// Ensure the namespace and relation for the resource and subject exist.
		if err := namespace.CheckNamespaceAndRelation(
			ctx,
			update.Tuple.ResourceAndRelation.Namespace,
			update.Tuple.ResourceAndRelation.Relation,
			false,
			reader,
		); err != nil {
			return err
		}

		if err := namespace.CheckNamespaceAndRelation(
			ctx,
			update.Tuple.Subject.Namespace,
			update.Tuple.Subject.Relation,
			true,
			reader,
		); err != nil {
			return err
		}

		// Build the type system for the object type.
		_, ts, err := namespace.ReadNamespaceAndTypes(
			ctx,
			update.Tuple.ResourceAndRelation.Namespace,
			reader,
		)
		if err != nil {
			return err
		}

		// Validate that the relationship is not writing to a permission.
		if ts.IsPermission(update.Tuple.ResourceAndRelation.Relation) {
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

		isAllowed, err := ts.HasAllowedRelation(
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
		// TODO(jschorr): once caveats are supported on all datastores, we should elide this check if the
		// provided context is empty, as the allowed relation check above will ensure the caveat exists.
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
