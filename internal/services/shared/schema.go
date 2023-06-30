package shared

import (
	"context"

	"github.com/authzed/spicedb/internal/caveats"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidatedSchemaChanges is a set of validated schema changes that can be applied to the datastore.
type ValidatedSchemaChanges struct {
	compiled          *compiler.CompiledSchema
	newCaveatDefNames *mapz.Set[string]
	newObjectDefNames *mapz.Set[string]
	additiveOnly      bool
}

// ValidateSchemaChanges validates the schema found in the compiled schema and returns a
// ValidatedSchemaChanges, if fully validated.
func ValidateSchemaChanges(ctx context.Context, compiled *compiler.CompiledSchema, additiveOnly bool) (*ValidatedSchemaChanges, error) {
	// 1) Validate the caveats defined.
	newCaveatDefNames := mapz.NewSet[string]()
	for _, caveatDef := range compiled.CaveatDefinitions {
		if err := namespace.ValidateCaveatDefinition(caveatDef); err != nil {
			return nil, err
		}

		newCaveatDefNames.Add(caveatDef.Name)
	}

	// 2) Validate the namespaces defined.
	newObjectDefNames := mapz.NewSet[string]()
	for _, nsdef := range compiled.ObjectDefinitions {
		ts, err := namespace.NewNamespaceTypeSystem(nsdef,
			namespace.ResolverForPredefinedDefinitions(namespace.PredefinedElements{
				Namespaces: compiled.ObjectDefinitions,
				Caveats:    compiled.CaveatDefinitions,
			}))
		if err != nil {
			return nil, err
		}

		vts, err := ts.Validate(ctx)
		if err != nil {
			return nil, err
		}

		if err := namespace.AnnotateNamespace(vts); err != nil {
			return nil, err
		}

		newObjectDefNames.Add(nsdef.Name)
	}

	return &ValidatedSchemaChanges{
		compiled:          compiled,
		newCaveatDefNames: newCaveatDefNames,
		newObjectDefNames: newObjectDefNames,
		additiveOnly:      additiveOnly,
	}, nil
}

// AppliedSchemaChanges holds information about the applied schema changes.
type AppliedSchemaChanges struct {
	// TotalOperationCount holds the total number of "dispatch" operations performed by the schema
	// being applied.
	TotalOperationCount uint32

	// NewObjectDefNames contains the names of the newly added object definitions.
	NewObjectDefNames []string

	// RemovedObjectDefNames contains the names of the removed object definitions.
	RemovedObjectDefNames []string

	// NewCaveatDefNames contains the names of the newly added caveat definitions.
	NewCaveatDefNames []string

	// RemovedCaveatDefNames contains the names of the removed caveat definitions.
	RemovedCaveatDefNames []string
}

// ApplySchemaChanges applies schema changes found in the validated changes struct, via the specified
// ReadWriteTransaction.
func ApplySchemaChanges(ctx context.Context, rwt datastore.ReadWriteTransaction, validated *ValidatedSchemaChanges) (*AppliedSchemaChanges, error) {
	existingCaveats, err := rwt.ListAllCaveats(ctx)
	if err != nil {
		return nil, err
	}

	existingObjectDefs, err := rwt.ListAllNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	return ApplySchemaChangesOverExisting(ctx, rwt, validated, datastore.DefinitionsOf(existingCaveats), datastore.DefinitionsOf(existingObjectDefs))
}

// ApplySchemaChangesOverExisting applies schema changes found in the validated changes struct, against
// existing caveat and object definitions given.
func ApplySchemaChangesOverExisting(
	ctx context.Context,
	rwt datastore.ReadWriteTransaction,
	validated *ValidatedSchemaChanges,
	existingCaveats []*core.CaveatDefinition,
	existingObjectDefs []*core.NamespaceDefinition,
) (*AppliedSchemaChanges, error) {
	// Build a map of existing caveats to determine those being removed, if any.
	existingCaveatDefMap := make(map[string]*core.CaveatDefinition, len(existingCaveats))
	existingCaveatDefNames := mapz.NewSet[string]()

	for _, existingCaveat := range existingCaveats {
		existingCaveatDefMap[existingCaveat.Name] = existingCaveat
		existingCaveatDefNames.Add(existingCaveat.Name)
	}

	// For each caveat definition, perform a diff and ensure the changes will not result in type errors.
	caveatDefsWithChanges := make([]*core.CaveatDefinition, 0, len(validated.compiled.CaveatDefinitions))
	for _, caveatDef := range validated.compiled.CaveatDefinitions {
		diff, err := sanityCheckCaveatChanges(ctx, rwt, caveatDef, existingCaveatDefMap)
		if err != nil {
			return nil, err
		}

		if len(diff.Deltas()) > 0 {
			caveatDefsWithChanges = append(caveatDefsWithChanges, caveatDef)
		}
	}

	removedCaveatDefNames := existingCaveatDefNames.Subtract(validated.newCaveatDefNames)

	// Build a map of existing definitions to determine those being removed, if any.
	existingObjectDefMap := make(map[string]*core.NamespaceDefinition, len(existingObjectDefs))
	existingObjectDefNames := mapz.NewSet[string]()
	for _, existingDef := range existingObjectDefs {
		existingObjectDefMap[existingDef.Name] = existingDef
		existingObjectDefNames.Add(existingDef.Name)
	}

	// For each definition, perform a diff and ensure the changes will not result in any
	// breaking changes.
	objectDefsWithChanges := make([]*core.NamespaceDefinition, 0, len(validated.compiled.ObjectDefinitions))
	for _, nsdef := range validated.compiled.ObjectDefinitions {
		diff, err := sanityCheckNamespaceChanges(ctx, rwt, nsdef, existingObjectDefMap)
		if err != nil {
			return nil, err
		}

		if len(diff.Deltas()) > 0 {
			objectDefsWithChanges = append(objectDefsWithChanges, nsdef)
		}
	}

	log.Ctx(ctx).
		Trace().
		Int("objectDefinitions", len(validated.compiled.ObjectDefinitions)).
		Int("caveatDefinitions", len(validated.compiled.CaveatDefinitions)).
		Int("objectDefsWithChanges", len(objectDefsWithChanges)).
		Int("caveatDefsWithChanges", len(caveatDefsWithChanges)).
		Msg("validated namespace definitions")

	// Ensure that deleting namespaces will not result in any relationships left without associated
	// schema.
	removedObjectDefNames := existingObjectDefNames.Subtract(validated.newObjectDefNames)
	if !validated.additiveOnly {
		if err := removedObjectDefNames.ForEach(func(nsdefName string) error {
			return ensureNoRelationshipsExist(ctx, rwt, nsdefName)
		}); err != nil {
			return nil, err
		}
	}

	// Write the new/changes caveats.
	if len(caveatDefsWithChanges) > 0 {
		if err := rwt.WriteCaveats(ctx, caveatDefsWithChanges); err != nil {
			return nil, err
		}
	}

	// Write the new/changed namespaces.
	if len(objectDefsWithChanges) > 0 {
		if err := rwt.WriteNamespaces(ctx, objectDefsWithChanges...); err != nil {
			return nil, err
		}
	}

	if !validated.additiveOnly {
		// Delete the removed namespaces.
		if removedObjectDefNames.Len() > 0 {
			if err := rwt.DeleteNamespaces(ctx, removedObjectDefNames.AsSlice()...); err != nil {
				return nil, err
			}
		}

		// Delete the removed caveats.
		if !removedCaveatDefNames.IsEmpty() {
			if err := rwt.DeleteCaveats(ctx, removedCaveatDefNames.AsSlice()); err != nil {
				return nil, err
			}
		}
	}

	log.Ctx(ctx).Trace().
		Interface("objectDefinitions", validated.compiled.ObjectDefinitions).
		Interface("caveatDefinitions", validated.compiled.CaveatDefinitions).
		Object("addedOrChangedObjectDefinitions", validated.newObjectDefNames).
		Object("removedObjectDefinitions", removedObjectDefNames).
		Object("addedOrChangedCaveatDefinitions", validated.newCaveatDefNames).
		Object("removedCaveatDefinitions", removedCaveatDefNames).
		Msg("completed schema update")

	return &AppliedSchemaChanges{
		TotalOperationCount:   uint32(len(validated.compiled.ObjectDefinitions) + len(validated.compiled.CaveatDefinitions) + removedObjectDefNames.Len() + removedCaveatDefNames.Len()),
		NewObjectDefNames:     validated.newObjectDefNames.Subtract(existingObjectDefNames).AsSlice(),
		RemovedObjectDefNames: removedObjectDefNames.AsSlice(),
		NewCaveatDefNames:     validated.newCaveatDefNames.Subtract(existingCaveatDefNames).AsSlice(),
		RemovedCaveatDefNames: removedCaveatDefNames.AsSlice(),
	}, nil
}

// sanityCheckCaveatChanges ensures that a caveat definition being written does not break
// the types of the parameters that may already exist on relationships.
func sanityCheckCaveatChanges(
	_ context.Context,
	_ datastore.ReadWriteTransaction,
	caveatDef *core.CaveatDefinition,
	existingDefs map[string]*core.CaveatDefinition,
) (*caveats.Diff, error) {
	// Ensure that the updated namespace does not break the existing tuple data.
	existing := existingDefs[caveatDef.Name]
	diff, err := caveats.DiffCaveats(existing, caveatDef)
	if err != nil {
		return nil, err
	}

	for _, delta := range diff.Deltas() {
		switch delta.Type {
		case caveats.RemovedParameter:
			return diff, NewSchemaWriteDataValidationError("cannot remove parameter `%s` on caveat `%s`", delta.ParameterName, caveatDef.Name)

		case caveats.ParameterTypeChanged:
			return diff, NewSchemaWriteDataValidationError("cannot change the type of parameter `%s` on caveat `%s`", delta.ParameterName, caveatDef.Name)
		}
	}

	return diff, nil
}

// ensureNoRelationshipsExist ensures that no relationships exist within the namespace with the given name.
func ensureNoRelationshipsExist(ctx context.Context, rwt datastore.ReadWriteTransaction, namespaceName string) error {
	qy, qyErr := rwt.QueryRelationships(
		ctx,
		datastore.RelationshipsFilter{ResourceType: namespaceName},
		options.WithLimit(options.LimitOne),
	)
	if err := errorIfTupleIteratorReturnsTuples(
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
	}, options.WithLimitForReverse(options.LimitOne))
	err := errorIfTupleIteratorReturnsTuples(
		ctx,
		qy,
		qyErr,
		"cannot delete object definition `%s`, as a relationship references it",
		namespaceName,
	)
	qy.Close()
	if err != nil {
		return err
	}

	return nil
}

// sanityCheckNamespaceChanges ensures that a namespace definition being written does not result
// in breaking changes, such as relationships without associated defined schema object definitions
// and relations.
func sanityCheckNamespaceChanges(
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

			err = errorIfTupleIteratorReturnsTuples(
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
			}, options.WithLimitForReverse(options.LimitOne))
			err = errorIfTupleIteratorReturnsTuples(
				ctx,
				qy,
				qyErr,
				"cannot delete relation `%s` in object definition `%s`, as a relationship references it", delta.RelationName, nsdef.Name)
			qy.Close()
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

			qyr, qyrErr := rwt.QueryRelationships(
				ctx,
				datastore.RelationshipsFilter{
					ResourceType:             nsdef.Name,
					OptionalResourceRelation: delta.RelationName,
					OptionalSubjectsSelectors: []datastore.SubjectsSelector{
						{
							OptionalSubjectType: delta.AllowedType.Namespace,
							OptionalSubjectIds:  optionalSubjectIds,
							RelationFilter:      relationFilter,
						},
					},
					OptionalCaveatName: optionalCaveatName,
				},
				options.WithLimit(options.LimitOne),
			)
			err = errorIfTupleIteratorReturnsTuples(
				ctx,
				qyr,
				qyrErr,
				"cannot remove allowed type `%s` from relation `%s` in object definition `%s`, as a relationship exists with it",
				namespace.SourceForAllowedRelation(delta.AllowedType), delta.RelationName, nsdef.Name)
			qyr.Close()
			if err != nil {
				return diff, err
			}
		}
	}
	return diff, nil
}

// errorIfTupleIteratorReturnsTuples takes a tuple iterator and any error that was generated
// when the original iterator was created, and returns an error if iterator contains any tuples.
func errorIfTupleIteratorReturnsTuples(_ context.Context, qy datastore.RelationshipIterator, qyErr error, message string, args ...interface{}) error {
	if qyErr != nil {
		return qyErr
	}
	defer qy.Close()

	if rt := qy.Next(); rt != nil {
		if qy.Err() != nil {
			return qy.Err()
		}

		return NewSchemaWriteDataValidationError(message, args...)
	}
	return nil
}
